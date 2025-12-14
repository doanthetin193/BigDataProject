# ==========================================================
#  prophet_train.py
#  Week 4 - Forecasting BTC/ETH using Prophet + Spark
#  Author: Doan The Tin (Big Data Project)
# ==========================================================

from pyspark.sql import SparkSession
import pandas as pd
from prophet import Prophet
from prophet.diagnostics import cross_validation
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt
import os
import itertools
import time
import logging
import plotly.express as px  # ✅ thêm dòng này để tạo biểu đồ interactive

# ==========================================================
# 1️⃣ Setup Logging
# ==========================================================
base_dir = r"D:\BigDataProject"
logs_dir = os.path.join(base_dir, "logs")
os.makedirs(logs_dir, exist_ok=True)
log_path = os.path.join(logs_dir, "prophet_train.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_path, mode="w", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==========================================================
# 2️⃣ Spark Configuration
# ==========================================================
spark = (
    SparkSession.builder
    .appName("ProphetTrain")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "50")
    .config("spark.executor.heartbeatInterval", "60s")
    .config("spark.network.timeout", "120s")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ==========================================================
# 3️⃣ Paths Setup + Auto Backup
# ==========================================================
analysis_dir = os.path.join(base_dir, "data_analysis")
prophet_path = os.path.join(analysis_dir, "prophet_input")
daily_filled_path = os.path.join(analysis_dir, "daily_filled")

forecasts_dir = os.path.join(analysis_dir, "prophet_forecasts")
metrics_dir = os.path.join(analysis_dir, "prophet_metrics")
vis_path = os.path.join(analysis_dir, "prophet_visualizations")
results_path = os.path.join(analysis_dir, "prophet_results")

# Backup directories (optional - có thể bỏ nếu không cần)
forecasts_dir_old = os.path.join(analysis_dir, "prophet_forecasts_backup")
metrics_dir_old = os.path.join(analysis_dir, "prophet_metrics_backup")
vis_path_old = os.path.join(analysis_dir, "prophet_visualizations_backup")
results_path_old = os.path.join(analysis_dir, "prophet_results_backup")

# Auto backup: nếu thư mục new có file → copy sang old trước khi ghi đè
import shutil
for new_dir, old_dir in [
    (forecasts_dir, forecasts_dir_old),
    (metrics_dir, metrics_dir_old),
    (vis_path, vis_path_old),
    (results_path, results_path_old)
]:
    if os.path.exists(new_dir) and os.listdir(new_dir):
        # Có file trong thư mục new → backup sang old
        if os.path.exists(old_dir):
            shutil.rmtree(old_dir)  # Xóa old cũ
        shutil.copytree(new_dir, old_dir)  # Copy new → old
        logger.info(f"✅ Backed up {os.path.basename(new_dir)} → {os.path.basename(old_dir)}")

for p in [forecasts_dir, metrics_dir, vis_path, results_path]:
    os.makedirs(p, exist_ok=True)

# ==========================================================
# 4️⃣ Helper: Train Prophet cho từng symbol
# ==========================================================
def train_prophet(pdf_sym, sym):
    """Train Prophet model cho 1 symbol và trả về forecast + metrics."""
    start_time = time.time()
    logger.info(f"=== Processing {sym} ===")

    pdf_sym = pdf_sym.sort_values("ds")
    split_idx = int(len(pdf_sym) * 0.8)
    train = pdf_sym.iloc[:split_idx][["ds", "y", "ma7", "ma30"]]
    test = pdf_sym.iloc[split_idx:][["ds", "y", "ma7", "ma30"]]

    logger.info(f"{sym} - Train: {len(train)} rows, Test: {len(test)} rows")

    # Holidays (chỉ cho BTC)
    holidays = None
    if sym == "BTCUSDT":
        holidays = pd.DataFrame({
            "holiday": "btc_halving",
            "ds": pd.to_datetime(["2016-07-09", "2020-05-11", "2024-04-20"]),
            "lower_window": -7,
            "upper_window": 7
        })

    # Grid search
    season_modes = ["additive", "multiplicative"]
    changepoint_priors = [0.01, 0.05, 0.1]
    grid = list(itertools.product(season_modes, changepoint_priors))
    
    best_mape = float("inf")
    best_model = None
    best_forecast = None
    best_params = None
    best_mse = None

    for mode, prior in grid:
        m = Prophet(
            seasonality_mode=mode,
            changepoint_prior_scale=prior,
            daily_seasonality=True,
            holidays=holidays
        )
        m.add_regressor("ma7")
        m.add_regressor("ma30")

        try:
            m.fit(train[["ds", "y", "ma7", "ma30"]])
        except Exception as e:
            logger.warning(f"{sym} - Fit failed for mode={mode}, prior={prior}: {str(e)}")
            continue
        
        # Forecast
        future = m.make_future_dataframe(periods=len(test) + 30, freq="D")
        # --- Ensure datetime alignment before merging ---
        pdf_sym["ds"] = pd.to_datetime(pdf_sym["ds"], errors="coerce")
        future["ds"] = pd.to_datetime(future["ds"], errors="coerce")

        future = future.merge(pdf_sym[["ds", "ma7", "ma30"]], on="ds", how="left")

        # ✅ Fix FutureWarning: dùng .ffill() thay vì fillna(method='ffill')
        future[["ma7", "ma30"]] = future[["ma7", "ma30"]].ffill().fillna(0.0)
        forecast = m.predict(future)
        
        # Evaluate
        pred_test = forecast.iloc[split_idx:split_idx + len(test)][["ds", "yhat"]]
        pred_test["ds"] = pd.to_datetime(pred_test["ds"], errors="coerce")
        test["ds"] = pd.to_datetime(test["ds"], errors="coerce")

        merged = pred_test.merge(test[["ds", "y"]], on="ds", how="left")
        merged["error"] = merged["y"] - merged["yhat"]
        merged["abs_error"] = merged["error"].abs()
        merged["pct_error"] = merged["abs_error"] / merged["y"].clip(lower=1e-8) * 100
        
        mse = mean_squared_error(merged["y"], merged["yhat"])
        mape = merged["pct_error"].mean()
        
        if mape < best_mape:
            best_mape = mape
            best_model = m
            best_forecast = forecast
            best_params = (mode, prior)
            best_mse = mse

    if best_model is None:
        logger.error(f"{sym} - No model trained successfully")
        return None

    # Cross-validation (30-day horizon)
    try:
        cv_df = cross_validation(
            best_model,
            horizon="30 days",
            period="15 days",
            initial=f"{len(train) - 60} days",
            parallel="threads"
        )
        cv_mape = (abs(cv_df["y"] - cv_df["yhat"]) / cv_df["y"].clip(lower=1e-8) * 100).mean()
        logger.info(f"{sym} - CV MAPE: {cv_mape:.3f}%")
    except Exception as e:
        cv_mape = None
        logger.warning(f"{sym} - Cross-validation failed: {str(e)}")

    # Save forecast & results
    forecast_out = os.path.join(forecasts_dir, f"{sym}_forecast.parquet")
    try:
        best_forecast.to_parquet(forecast_out, engine="pyarrow")
        logger.info(f"{sym} - Saved forecast to {forecast_out}")
    except Exception as e:
        logger.warning(f"{sym} - Failed to save forecast: {str(e)}")

    merged_out = os.path.join(results_path, f"{sym}_actual_vs_pred.csv")
    merged.to_csv(merged_out, index=False)
    logger.info(f"{sym} - Saved actual vs pred to {merged_out}")

    # ==========================================================
    # 📈 Plot: Forecast (Matplotlib PNG + Plotly HTML)
    # ==========================================================
    plt.figure(figsize=(12, 6))
    plt.plot(best_forecast["ds"], best_forecast["yhat"], label="Predicted")
    plt.plot(test["ds"], test["y"], label="Actual")
    plt.fill_between(
        best_forecast["ds"],
        best_forecast["yhat_lower"],
        best_forecast["yhat_upper"],
        alpha=0.1,
        label="Confidence Interval"
    )
    plt.title(f"{sym} Forecast vs Actual")
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(vis_path, f"{sym}_forecast.png"))
    plt.close()

    # ✅ Thêm đoạn Plotly interactive (3 dòng chính)
    fig_plotly = px.line(best_forecast, x="ds", y="yhat",
                         title=f"{sym} Forecast (Interactive)",
                         labels={"ds": "Date", "yhat": "Predicted Price"})
    fig_plotly.write_html(os.path.join(vis_path, f"{sym}_forecast_interactive.html"))

    # Plot: Test zoom
    plt.figure(figsize=(12, 6))
    plt.plot(merged["ds"], merged["yhat"], label="Predicted")
    plt.plot(merged["ds"], merged["y"], label="Actual")
    plt.title(f"{sym} Test Set: Actual vs Predicted")
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(os.path.join(vis_path, f"{sym}_test_zoom.png"))
    plt.close()

    # Components
    fig2 = best_model.plot_components(best_forecast)
    plt.savefig(os.path.join(vis_path, f"{sym}_components.png"))
    plt.close()

    elapsed = time.time() - start_time
    logger.info(f"{sym} - Completed in {elapsed/60:.2f} minutes")

    return {
        "symbol": sym,
        "mse": best_mse,
        "mape": best_mape,
        "cv_mape": cv_mape,
        "mode": best_params[0],
        "prior": best_params[1]
    }

# ==========================================================
# 5️⃣ Main Pipeline
# ==========================================================
logger.info("=== Prophet Forecasting Pipeline Start ===")

# Kiểm tra engine parquet
try:
    import pyarrow
    logger.info(f"pyarrow version: {pyarrow.__version__}")
except ImportError:
    logger.warning("⚠️ pyarrow not installed — run: pip install pyarrow")

# Load dữ liệu Prophet input + MA7/MA30 từ daily_filled (like original)
df = spark.read.parquet(prophet_path).select("ds", "y", "symbol")
daily_filled = (
    spark.read.parquet(daily_filled_path)
    .withColumnRenamed("date", "ds")
    .select("ds", "symbol", "ma7", "ma30")
)

df = df.join(daily_filled, on=["ds", "symbol"], how="left").cache()

expected_cols = {"ds", "y", "symbol", "ma7", "ma30"}
if not expected_cols.issubset(set(df.columns)):
    raise ValueError(f"Input parquet thiếu các cột: {expected_cols - set(df.columns)}")

symbols = df.select("symbol").distinct().toPandas()["symbol"].tolist()
logger.info(f"Loaded symbols: {symbols}")

metrics_list = []
for sym in symbols:
    pdf_sym = df.filter(df.symbol == sym).toPandas()
    if pdf_sym.empty:
        logger.warning(f"{sym} - Empty DataFrame, skipping")
        continue
    metrics = train_prophet(pdf_sym, sym)
    if metrics:
        metrics_list.append(metrics)

# Save metrics
metrics_df = pd.DataFrame(metrics_list)
metrics_path = os.path.join(metrics_dir, "metrics.csv")
metrics_df.to_csv(metrics_path, index=False)
logger.info(f"Saved metrics to: {metrics_path}")

logger.info("\n=== Summary ===")
logger.info(metrics_df.to_string(index=False))

df.unpersist()
spark.stop()

logger.info("\n✅ Forecast & evaluation complete!")
