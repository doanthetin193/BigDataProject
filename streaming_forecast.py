"""
streaming_forecast.py - Dự báo real-time với Prophet sử dụng dữ liệu streaming
Update model với dữ liệu mới và tạo forecast mới nhất
"""
import pandas as pd
from prophet import Prophet
import matplotlib.pyplot as plt
import plotly.express as px
import os
from datetime import datetime, timedelta

# ==========================================================
# CONFIG
# ==========================================================
BASE_DIR = r"D:\BigDataProject"
OUTPUT_DIR = os.path.join(BASE_DIR, "streaming_output")

# Tạo thư mục với timestamp để lưu kết quả mỗi lần chạy
RUN_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
FORECAST_DIR = os.path.join(BASE_DIR, "data_analysis", "streaming_forecasts", RUN_TIMESTAMP)
VIS_DIR = os.path.join(BASE_DIR, "data_analysis", "streaming_visualizations", RUN_TIMESTAMP)

for d in [FORECAST_DIR, VIS_DIR]:
    os.makedirs(d, exist_ok=True)

# Symlink hoặc copy "latest" folder cho dễ access
FORECAST_LATEST = os.path.join(BASE_DIR, "data_analysis", "streaming_forecasts", "latest")
VIS_LATEST = os.path.join(BASE_DIR, "data_analysis", "streaming_visualizations", "latest")

print("=" * 70)
print(" REAL-TIME FORECASTING WITH PROPHET + STREAMING DATA")
print("=" * 70)

# ==========================================================
# FORECAST FUNCTION
# ==========================================================
def forecast_with_streaming_data(symbol):
    """
    Huấn luyện Prophet với dữ liệu merged (historical + streaming)
    và tạo forecast mới nhất
    """
    print(f"\n Forecasting {symbol} with updated data")
    print("-" * 70)
    
    # Load updated daily data (CSV from pandas_processor)
    daily_csv = os.path.join(OUTPUT_DIR, f"{symbol}_daily_updated.csv")
    
    if not os.path.exists(daily_csv):
        print(f"   Updated daily data not found: {daily_csv}")
        print(f"     Run pandas_processor.py first!")
        return None
    
    # Read data
    pdf = pd.read_csv(daily_csv)
    pdf['date'] = pd.to_datetime(pdf['date'])
    
    # Calculate MA7 and MA30
    pdf = pdf.sort_values('date')
    pdf['ma7'] = pdf['close'].rolling(window=7, min_periods=1).mean()
    pdf['ma30'] = pdf['close'].rolling(window=30, min_periods=1).mean()
    
    # Rename columns for Prophet
    pdf = pdf.rename(columns={'date': 'ds', 'close': 'y'})
    pdf = pdf.sort_values("ds")
    
    print(f"   Loaded {len(pdf):,} days of data")
    print(f"   Range: {pdf['ds'].min()} -> {pdf['ds'].max()}")
    
    # Hiển thị dữ liệu mới nhất (30 ngày gần nhất)
    print(f"\n   Latest 30 days:")
    print(pdf.tail(30)[["ds", "y", "ma7", "ma30"]].to_string(index=False))
    
    # Train/Test split (80/20)
    split_idx = int(len(pdf) * 0.8)
    train = pdf.iloc[:split_idx]
    test = pdf.iloc[split_idx:]
    
    print(f"\n   Training data: {len(train)} days")
    print(f"   Test data: {len(test)} days")
    
    # Configure Prophet
    model = Prophet(
        seasonality_mode="multiplicative",
        changepoint_prior_scale=0.05,
        daily_seasonality=True,
        yearly_seasonality=True,
        weekly_seasonality=True
    )
    
    # Add regressors
    model.add_regressor("ma7")
    model.add_regressor("ma30")
    
    # Holidays for BTC
    holidays = None
    if symbol == "BTCUSDT":
        holidays = pd.DataFrame({
            "holiday": "btc_halving",
            "ds": pd.to_datetime(["2016-07-09", "2020-05-11", "2024-04-20"]),
            "lower_window": -7,
            "upper_window": 7
        })
        model.holidays = holidays
    
    # Train model
    print(f"\n   Training Prophet model...")
    model.fit(train[["ds", "y", "ma7", "ma30"]])
    print(f"   Model trained successfully!")
    
    # Evaluate on test set
    test_future = test[["ds", "ma7", "ma30"]].copy()
    test_forecast = model.predict(test_future)
    test_merged = test_forecast[["ds", "yhat"]].merge(test[["ds", "y"]], on="ds")
    
    mse = ((test_merged["y"] - test_merged["yhat"]) ** 2).mean()
    mae = (test_merged["y"] - test_merged["yhat"]).abs().mean()
    mape = ((test_merged["y"] - test_merged["yhat"]).abs() / test_merged["y"]).mean() * 100
    
    print(f"\n   Model Performance (Test Set):")
    print(f"     MSE:  {mse:,.2f}")
    print(f"     MAE:  {mae:,.2f}")
    print(f"     MAPE: {mape:.2f}%")
    
    # Retrain on full dataset for final forecast (need new model instance)
    print(f"\n   Retraining on full dataset for forecast...")
    model_full = Prophet(
        seasonality_mode="multiplicative",
        changepoint_prior_scale=0.05,
        daily_seasonality=True,
        yearly_seasonality=True,
        weekly_seasonality=True
    )
    model_full.add_regressor('ma7')
    model_full.add_regressor('ma30')
    
    # Add holidays if exists
    if holidays is not None:
        model_full.holidays = holidays
    
    model_full.fit(pdf[["ds", "y", "ma7", "ma30"]])
    
    # Make forecast (30 days into future from last known date)
    future = model_full.make_future_dataframe(periods=30, freq="D")
    
    # Merge MA features
    pdf["ds"] = pd.to_datetime(pdf["ds"])
    future["ds"] = pd.to_datetime(future["ds"])
    future = future.merge(pdf[["ds", "ma7", "ma30"]], on="ds", how="left")
    
    # Forward fill MA for future dates
    future[["ma7", "ma30"]] = future[["ma7", "ma30"]].ffill()
    
    # Forecast with full model
    forecast = model_full.predict(future)
    
    # Future predictions (next 30 days after last known date)
    last_date = pdf["ds"].max()
    future_pred = forecast[forecast["ds"] > last_date][["ds", "yhat", "yhat_lower", "yhat_upper"]].head(30)
    
    print(f"\n   Forecast for next 30 days:")
    if len(future_pred) > 0:
        print(future_pred.to_string(index=False))
    else:
        print("   No future predictions generated")
    
    # Save forecast
    forecast_path = os.path.join(FORECAST_DIR, f"{symbol}_streaming_forecast.parquet")
    forecast.to_parquet(forecast_path, engine="pyarrow")
    print(f"\n   Forecast saved to {forecast_path}")
    
    # Save metrics
    metrics = pd.DataFrame([{
        "symbol": symbol,
        "train_days": len(train),
        "test_days": len(test),
        "mse": mse,
        "mae": mae,
        "mape": mape,
        "last_date": pdf["ds"].max().strftime("%Y-%m-%d"),
        "forecast_days": 30,
        "run_timestamp": RUN_TIMESTAMP,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }])
    
    metrics_path = os.path.join(FORECAST_DIR, f"{symbol}_metrics.csv")
    metrics.to_csv(metrics_path, index=False)
    print(f"   Metrics saved to {metrics_path}")
    
    # Visualizations
    create_visualizations(symbol, pdf, forecast, test_merged, future_pred)
    
    return {
        "symbol": symbol,
        "mse": mse,
        "mae": mae,
        "mape": mape,
        "forecast": forecast
    }

# ==========================================================
# VISUALIZATION
# ==========================================================
def create_visualizations(symbol, historical, forecast, test_merged, future_pred):
    """Tạo biểu đồ cho forecast"""
    print(f"\n   Creating visualizations...")
    
    # 1. Full forecast plot
    fig, ax = plt.subplots(figsize=(15, 6))
    
    # Historical data (last 90 days)
    hist_90 = historical.tail(90)
    ax.plot(hist_90["ds"], hist_90["y"], label="Historical", color="black", linewidth=1.5)
    
    # Forecast
    ax.plot(forecast["ds"], forecast["yhat"], label="Forecast", color="blue", linewidth=1.5, alpha=0.7)
    ax.fill_between(
        forecast["ds"],
        forecast["yhat_lower"],
        forecast["yhat_upper"],
        alpha=0.2,
        color="blue",
        label="Uncertainty"
    )
    
    # Future predictions
    ax.plot(future_pred["ds"], future_pred["yhat"], 
            label="Future (30 days)", color="red", linewidth=2, linestyle="--", marker="o", markersize=3)
    
    ax.set_title(f"{symbol} - Real-time Forecast (Updated with Streaming Data)", fontsize=14, fontweight="bold")
    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Price (USD)", fontsize=12)
    ax.legend(loc="best")
    ax.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    plot_path = os.path.join(VIS_DIR, f"{symbol}_streaming_forecast.png")
    plt.savefig(plot_path, dpi=300)
    plt.close()
    print(f"     Saved forecast plot: {plot_path}")
    
    # 2. Test set comparison
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(test_merged["ds"], test_merged["y"], label="Actual", color="black", linewidth=2, marker="o")
    ax.plot(test_merged["ds"], test_merged["yhat"], label="Predicted", color="blue", linewidth=2, marker="x")
    ax.set_title(f"{symbol} - Test Set: Actual vs Predicted", fontsize=14, fontweight="bold")
    ax.set_xlabel("Date")
    ax.set_ylabel("Price (USD)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    test_plot_path = os.path.join(VIS_DIR, f"{symbol}_test_comparison.png")
    plt.savefig(test_plot_path, dpi=300)
    plt.close()
    print(f"     Saved test comparison: {test_plot_path}")
    
    # 3. Interactive Plotly chart
    fig_interactive = px.line(
        forecast,
        x="ds",
        y="yhat",
        title=f"{symbol} Interactive Forecast (with Streaming Data)",
        labels={"ds": "Date", "yhat": "Predicted Price (USD)"}
    )
    
    # Add historical data
    fig_interactive.add_scatter(
        x=historical["ds"],
        y=historical["y"],
        mode="lines",
        name="Historical",
        line=dict(color="black", width=1)
    )
    
    # Add future predictions
    fig_interactive.add_scatter(
        x=future_pred["ds"],
        y=future_pred["yhat"],
        mode="lines+markers",
        name="Future (30 days)",
        line=dict(color="red", width=2, dash="dash")
    )
    
    interactive_path = os.path.join(VIS_DIR, f"{symbol}_interactive_forecast.html")
    fig_interactive.write_html(interactive_path)
    print(f"     Saved interactive chart: {interactive_path}")
    
    # 4. Residuals plot
    fig, ax = plt.subplots(figsize=(12, 5))
    residuals = test_merged["y"] - test_merged["yhat"]
    ax.scatter(test_merged["ds"], residuals, alpha=0.6, color="purple")
    ax.axhline(y=0, color="red", linestyle="--", linewidth=1)
    ax.set_title(f"{symbol} - Prediction Residuals", fontsize=14, fontweight="bold")
    ax.set_xlabel("Date")
    ax.set_ylabel("Residual (Actual - Predicted)")
    ax.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    residuals_path = os.path.join(VIS_DIR, f"{symbol}_residuals.png")
    plt.savefig(residuals_path, dpi=300)
    plt.close()
    print(f"     Saved residuals plot: {residuals_path}")

# ==========================================================
# MAIN EXECUTION
# ==========================================================
if __name__ == "__main__":
    symbols = ["BTCUSDT", "ETHUSDT"]
    results = []
    
    print("=" * 70)
    print(" REAL-TIME FORECASTING WITH PROPHET")
    print("=" * 70)
    print(f" Run Timestamp: {RUN_TIMESTAMP}")
    print(f" Forecast Output: {FORECAST_DIR}")
    print(f" Visualization Output: {VIS_DIR}")
    print("=" * 70)
    
    for symbol in symbols:
        result = forecast_with_streaming_data(symbol)
        if result:
            results.append(result)
    
    # Summary
    print("\n" + "=" * 70)
    print(" FORECASTING SUMMARY")
    print("=" * 70)
    
    if results:
        summary = pd.DataFrame([{
            "Symbol": r["symbol"],
            "MSE": f"{r['mse']:,.2f}",
            "MAE": f"{r['mae']:,.2f}",
            "MAPE": f"{r['mape']:.2f}%"
        } for r in results])
        
        print(summary.to_string(index=False))
    
    print("\n Real-time forecasting complete!")
    print(f" Forecasts: {FORECAST_DIR}")
    print(f" Visualizations: {VIS_DIR}")
    print(f" Run ID: {RUN_TIMESTAMP}")
    print("=" * 70)
    
    # Tạo symlink/shortcut đến "latest" để dễ truy cập
    import shutil
    try:
        # Copy kết quả mới nhất vào thư mục "latest"
        latest_forecast = os.path.join(BASE_DIR, "data_analysis", "streaming_forecasts", "latest")
        latest_vis = os.path.join(BASE_DIR, "data_analysis", "streaming_visualizations", "latest")
        
        # Remove old latest
        if os.path.exists(latest_forecast):
            shutil.rmtree(latest_forecast)
        if os.path.exists(latest_vis):
            shutil.rmtree(latest_vis)
        
        # Copy new latest
        shutil.copytree(FORECAST_DIR, latest_forecast)
        shutil.copytree(VIS_DIR, latest_vis)
        
        print(f"\n📌 Latest results also available at:")
        print(f"   {latest_forecast}")
        print(f"   {latest_vis}")
    except Exception as e:
        print(f"\n  Could not create 'latest' folder: {e}")
    
    print("=" * 70)
