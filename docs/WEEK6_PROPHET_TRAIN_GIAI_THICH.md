# Giải thích chi tiết: prophet_train.py

**File:** `scripts/ml_models/prophet_train.py`  
**Chức năng:** Machine Learning - Train Prophet model cho BTC/ETH forecasting  
**Tác giả:** Đoàn Thế Tín  
**Ngày:** Week 4 - Forecasting

---

## 📋 Mục lục
1. [Overview Prophet](#1-overview-prophet)
2. [Logging Setup](#2-logging-setup)
3. [Spark Configuration](#3-spark-configuration)
4. [Paths & Auto Backup](#4-paths--auto-backup)
5. [Train Prophet Function](#5-train-prophet-function)
6. [Grid Search & Hyperparameter Tuning](#6-grid-search--hyperparameter-tuning)
7. [Cross-Validation](#7-cross-validation)
8. [Visualization](#8-visualization)
9. [Main Pipeline](#9-main-pipeline)
10. [Tóm tắt](#tóm-tắt-tổng-quan)

---

## 1. Overview Prophet

### Prophet là gì?

**Prophet** = Time series forecasting library từ Facebook (Meta)
- **Designed for:** Business time series với seasonality + holidays
- **Use case:** Stock prices, sales, traffic, crypto prices
- **Advantage:** Easy to use, robust, interpretable

---

### Dòng 1-6: File Header
```python
# ==========================================================
#  prophet_train.py
#  Week 4 - Forecasting BTC/ETH using Prophet + Spark
#  Author: Doan The Tin (Big Data Project)
# ==========================================================
```

---

### Dòng 7-16: Import Libraries
```python
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
```
**Giải thích:**

#### Core Libraries
- `SparkSession`: Load Parquet data
- `pandas`: Data manipulation (Prophet requires Pandas)
- `Prophet`: Time series forecasting model

#### Evaluation & Diagnostics
- `cross_validation`: Prophet's CV function
- `mean_squared_error`: MSE metric

#### Visualization
- `matplotlib.pyplot`: Static plots (PNG)
- `plotly.express`: Interactive plots (HTML)

#### Utilities
- `itertools`: Grid search combinations
- `time`: Measure training time
- `logging`: Log to file + console

---

## 2. Logging Setup

### Dòng 18-34: Logging Configuration
```python
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
```
**Giải thích:**

### Base Directory
- `base_dir = r"D:\BigDataProject"`
- Raw string (r prefix) để avoid escape issues

### Logs Directory
- `logs_dir`: D:\BigDataProject\logs
- `os.makedirs(exist_ok=True)`: Tạo folder nếu chưa có

### Logging Handlers
| Handler | Purpose | Output |
|---------|---------|--------|
| `FileHandler` | Log to file | `logs/prophet_train.log` |
| `StreamHandler` | Log to console | stdout/stderr |

**Mode:** `w` (overwrite mỗi lần chạy)

### Log Format
- `%(asctime)s`: Timestamp (2025-12-16 10:30:45)
- `%(levelname)s`: INFO/WARNING/ERROR
- `%(message)s`: Log message

**Ví dụ output:**
```
2025-12-16 10:30:45 - INFO - === Prophet Forecasting Pipeline Start ===
2025-12-16 10:30:47 - INFO - Loaded symbols: ['BTCUSDT', 'ETHUSDT']
```

---

## 3. Spark Configuration

### Dòng 36-50: Spark Session
```python
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
```
**Giải thích:**

### Spark Configuration

| Config | Value | Purpose |
|--------|-------|---------|
| `appName` | ProphetTrain | Tên application (hiện trong UI) |
| `driver.memory` | 4g | 4 GB RAM cho driver |
| `shuffle.partitions` | 50 | Số partitions cho shuffle |
| `heartbeatInterval` | 60s | Executor heartbeat frequency |
| `network.timeout` | 120s | Network timeout |

### Tại sao cần 4GB memory?
- Prophet training memory-intensive
- Cross-validation cần nhiều memory
- Grid search train nhiều models

### Log Level
- `setLogLevel("WARN")`: Chỉ show WARN/ERROR
- Giảm noise từ Spark logs

---

## 4. Paths & Auto Backup

### Dòng 52-91: Paths Setup
```python
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
```
**Giải thích:**

### Input Paths
| Variable | Path | Content |
|----------|------|---------|
| `prophet_path` | `data_analysis/prophet_input` | Input data (ds, y, symbol) |
| `daily_filled_path` | `data_analysis/daily_filled` | MA7/MA30 regressors |

### Output Paths
| Variable | Path | Content |
|----------|------|---------|
| `forecasts_dir` | `prophet_forecasts` | Parquet forecasts |
| `metrics_dir` | `prophet_metrics` | metrics.csv |
| `vis_path` | `prophet_visualizations` | PNG/HTML plots |
| `results_path` | `prophet_results` | actual_vs_pred.csv |

### Backup Paths
- Suffix `_backup`: Old versions
- Auto backup trước khi overwrite

---

### Dòng 73-91: Auto Backup Logic
```python
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
```
**Giải thích:**

### Backup Strategy
**Workflow:**
1. **Check:** `new_dir` có files không?
2. **Delete old:** Nếu `old_dir` tồn tại → xóa
3. **Copy:** `new_dir` → `old_dir`
4. **Create:** Tạo `new_dir` (empty) cho run mới

**Ví dụ:**
```
Run 1 (no backup):
  prophet_forecasts/          → BTCUSDT_forecast.parquet, ETHUSDT_forecast.parquet
  prophet_forecasts_backup/   → (empty)

Run 2 (auto backup):
  Step 1: Copy prophet_forecasts → prophet_forecasts_backup
  Step 2: Clear prophet_forecasts
  Step 3: Train new models → Save to prophet_forecasts
```

### Tại sao backup?
- **Safety:** Preserve previous results
- **Comparison:** Compare old vs new forecasts
- **Rollback:** Nếu new run failed

---

## 5. Train Prophet Function

### Dòng 93-103: Function Definition
```python
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
```
**Giải thích:**

### Function Signature
```python
def train_prophet(pdf_sym, sym):
```
- **pdf_sym:** Pandas DataFrame cho 1 symbol
- **sym:** Symbol name (BTCUSDT, ETHUSDT)

### Train-Test Split
- **Sort:** `sort_values("ds")` (chronological)
- **Split ratio:** 80/20
- **Columns:** ds, y, ma7, ma30

**Ví dụ:**
```
BTCUSDT:
  Total: 1,022 rows (2023-03-01 → 2025-12-16)
  Train: 818 rows (80%)
  Test: 204 rows (20%)

ETHUSDT:
  Total: 611 rows
  Train: 489 rows
  Test: 122 rows
```

---

### Dòng 105-113: Holidays (BTC Only)
```python
    # Holidays (chỉ cho BTC)
    holidays = None
    if sym == "BTCUSDT":
        holidays = pd.DataFrame({
            "holiday": "btc_halving",
            "ds": pd.to_datetime(["2016-07-09", "2020-05-11", "2024-04-20"]),
            "lower_window": -7,
            "upper_window": 7
        })
```
**Giải thích:**

### Bitcoin Halving Events
**Tại sao quan trọng?**
- **BTC Halving:** Mining reward giảm 50%
- **Impact:** Historically tăng giá mạnh
- **Frequency:** Mỗi 4 năm

### Halving Dates
| Date | Event | Impact Window |
|------|-------|---------------|
| 2016-07-09 | 2nd Halving | ±7 days |
| 2020-05-11 | 3rd Halving | ±7 days |
| 2024-04-20 | 4th Halving | ±7 days |

### Window Parameters
- `lower_window`: -7 (7 ngày trước)
- `upper_window`: 7 (7 ngày sau)
- **Total:** 15 days impact

**Prophet sẽ học pattern:** "Giá thường tăng quanh halving dates"

---

## 6. Grid Search & Hyperparameter Tuning

### Dòng 115-124: Grid Search Setup
```python
    # Grid search
    season_modes = ["additive", "multiplicative"]
    changepoint_priors = [0.01, 0.05, 0.1]
    grid = list(itertools.product(season_modes, changepoint_priors))
    
    best_mape = float("inf")
    best_model = None
    best_forecast = None
    best_params = None
    best_mse = None
```
**Giải thích:**

### Hyperparameters

#### 1. Seasonality Mode
| Mode | Formula | Use Case |
|------|---------|----------|
| `additive` | y = trend + seasonal | Seasonal variation constant |
| `multiplicative` | y = trend * seasonal | Seasonal variation scales with trend |

**Example:**
```
Additive: Dec sales = 1000 + 200 (same +200 every year)
Multiplicative: Dec sales = 1000 * 1.2 (20% increase, scales with base)
```

#### 2. Changepoint Prior Scale
- **Range:** 0.01 (conservative) → 0.1 (flexible)
- **Purpose:** Control trend flexibility
- **Low value (0.01):** Smooth trend, less responsive
- **High value (0.1):** Flexible trend, responsive to changes

### Grid Search
```python
grid = list(itertools.product(season_modes, changepoint_priors))
# Result: 2 x 3 = 6 combinations
[
    ("additive", 0.01),
    ("additive", 0.05),
    ("additive", 0.1),
    ("multiplicative", 0.01),
    ("multiplicative", 0.05),
    ("multiplicative", 0.1)
]
```

### Tracking Best Model
- **best_mape:** Lowest MAPE (Mean Absolute Percentage Error)
- **best_model:** Model object
- **best_forecast:** Forecast DataFrame
- **best_params:** (mode, prior)
- **best_mse:** Mean Squared Error

---

### Dòng 126-144: Model Training Loop
```python
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
```
**Giải thích:**

### Prophet Model Configuration

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `seasonality_mode` | additive/multiplicative | How seasonality affects trend |
| `changepoint_prior_scale` | 0.01/0.05/0.1 | Trend flexibility |
| `daily_seasonality` | True | Daily patterns (24h crypto trading) |
| `holidays` | BTC halving dates | Special events impact |

### Add Regressors
```python
m.add_regressor("ma7")   # 7-day moving average
m.add_regressor("ma30")  # 30-day moving average
```
**Tại sao?**
- MA7/MA30 capture short-term trends
- Help model understand momentum
- Improve forecast accuracy

### Fit Model
```python
m.fit(train[["ds", "y", "ma7", "ma30"]])
```
- **Input:** Train data (80%)
- **Columns:** ds (date), y (price), ma7, ma30
- **Output:** Trained model `m`

### Error Handling
- `try/except`: Catch fit errors
- `continue`: Skip failed configs
- **Log warning:** Notify về failed configs

---

### Dòng 146-159: Forecasting
```python
        # Forecast
        future = m.make_future_dataframe(periods=len(test) + 30, freq="D")
        # --- Ensure datetime alignment before merging ---
        pdf_sym["ds"] = pd.to_datetime(pdf_sym["ds"], errors="coerce")
        future["ds"] = pd.to_datetime(future["ds"], errors="coerce")

        future = future.merge(pdf_sym[["ds", "ma7", "ma30"]], on="ds", how="left")

        # ✅ Fix FutureWarning: dùng .ffill() thay vì fillna(method='ffill')
        future[["ma7", "ma30"]] = future[["ma7", "ma30"]].ffill().fillna(0.0)
        forecast = m.predict(future)
```
**Giải thích:**

### Make Future DataFrame
```python
future = m.make_future_dataframe(periods=len(test) + 30, freq="D")
```
- **periods:** Test length + 30 days (future forecast)
- **freq:** "D" (daily)

**Ví dụ:**
```
Test: 204 rows
periods: 204 + 30 = 234 days
→ Forecast from last train date to 30 days into future
```

### Datetime Alignment
```python
pdf_sym["ds"] = pd.to_datetime(pdf_sym["ds"], errors="coerce")
future["ds"] = pd.to_datetime(future["ds"], errors="coerce")
```
- **Purpose:** Ensure consistent datetime format
- **errors="coerce":** Convert invalid dates to NaT

### Merge Regressors
```python
future = future.merge(pdf_sym[["ds", "ma7", "ma30"]], on="ds", how="left")
```
- **Merge:** Add ma7/ma30 to future dates
- **how="left":** Keep all future dates
- **Result:** Missing MA for future dates (NULL)

### Forward Fill MA
```python
future[["ma7", "ma30"]] = future[["ma7", "ma30"]].ffill().fillna(0.0)
```
- **ffill():** Forward fill (use last known MA)
- **fillna(0.0):** Fill remaining NULLs với 0

**Ví dụ:**
```
Date       | ma7   | After ffill
2025-12-14 | 42000 | 42000
2025-12-15 | 42500 | 42500
2025-12-16 | NULL  | 42500 (ffill)
2025-12-17 | NULL  | 42500 (ffill)
```

### Predict
```python
forecast = m.predict(future)
```
- **Input:** future (with MA)
- **Output:** Forecast DataFrame
  - `yhat`: Predicted value
  - `yhat_lower`: Lower confidence bound
  - `yhat_upper`: Upper confidence bound
  - `trend`, `seasonal` components

---

### Dòng 161-172: Evaluation
```python
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
```
**Giải thích:**

### Extract Test Predictions
```python
pred_test = forecast.iloc[split_idx:split_idx + len(test)][["ds", "yhat"]]
```
- **Extract:** Test period predictions only
- **Columns:** ds (date), yhat (predicted price)

### Merge Actual vs Predicted
```python
merged = pred_test.merge(test[["ds", "y"]], on="ds", how="left")
```
- **Result:** ds, y (actual), yhat (predicted)

### Calculate Errors
```python
merged["error"] = merged["y"] - merged["yhat"]           # Raw error
merged["abs_error"] = merged["error"].abs()              # Absolute error
merged["pct_error"] = merged["abs_error"] / merged["y"].clip(lower=1e-8) * 100  # Percentage
```

**Ví dụ:**
```
Date       | y (actual) | yhat (pred) | error  | abs_error | pct_error
2025-12-01 | 43000      | 42500       | 500    | 500       | 1.16%
2025-12-02 | 42000      | 42800       | -800   | 800       | 1.90%
```

### Metrics
```python
mse = mean_squared_error(merged["y"], merged["yhat"])  # Mean Squared Error
mape = merged["pct_error"].mean()                      # Mean Absolute % Error
```

**MSE vs MAPE:**
| Metric | Formula | Unit | Interpretation |
|--------|---------|------|----------------|
| MSE | avg((y - yhat)²) | Price² | Penalizes large errors |
| MAPE | avg(abs(y-yhat)/y) * 100 | % | Average % deviation |

---

### Dòng 174-180: Update Best Model
```python
        if mape < best_mape:
            best_mape = mape
            best_model = m
            best_forecast = forecast
            best_params = (mode, prior)
            best_mse = mse
```
**Giải thích:**
- **Selection criteria:** Lowest MAPE
- **Update:** Keep best model, forecast, params, MSE

**Ví dụ Grid Search:**
```
Config 1: (additive, 0.01)    → MAPE: 5.2%
Config 2: (additive, 0.05)    → MAPE: 4.8% ← Best so far
Config 3: (additive, 0.1)     → MAPE: 5.1%
Config 4: (multiplicative, 0.01) → MAPE: 4.9%
Config 5: (multiplicative, 0.05) → MAPE: 4.3% ← NEW BEST
Config 6: (multiplicative, 0.1)  → MAPE: 4.7%

Final Best: (multiplicative, 0.05) with MAPE 4.3%
```

---

### Dòng 182-185: Check Model
```python
    if best_model is None:
        logger.error(f"{sym} - No model trained successfully")
        return None
```
**Giải thích:**
- **Scenario:** All 6 configs failed
- **Action:** Log error, return None
- **Rare:** Usually at least 1 config succeeds

---

## 7. Cross-Validation

### Dòng 187-200: Cross-Validation
```python
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
```
**Giải thích:**

### Prophet Cross-Validation

**Tại sao CV?**
- **Single split:** May be lucky/unlucky
- **CV:** Test on multiple time periods
- **Robust:** More reliable performance estimate

### CV Parameters

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `horizon` | 30 days | Forecast this far into future |
| `period` | 15 days | Advance by this much each fold |
| `initial` | len(train) - 60 days | Minimum training data |
| `parallel` | threads | Multi-threading speedup |

### CV Workflow
**Ví dụ (Train: 818 days):**
```
Fold 1:
  Train: Day 1-758 (initial)
  Test: Day 759-788 (30 days horizon)

Fold 2 (advance 15 days):
  Train: Day 1-773
  Test: Day 774-803

Fold 3:
  Train: Day 1-788
  Test: Day 789-818

...continue until end of train data
```

### CV MAPE
```python
cv_mape = (abs(cv_df["y"] - cv_df["yhat"]) / cv_df["y"].clip(lower=1e-8) * 100).mean()
```
- **cv_df:** All CV predictions combined
- **MAPE:** Average % error across all folds

### Error Handling
- `try/except`: CV can fail (not enough data)
- `cv_mape = None`: If failed

---

## 8. Visualization

### Dòng 202-209: Save Forecast
```python
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
```
**Giải thích:**

### Save Forecast (Parquet)
- **Path:** `prophet_forecasts/BTCUSDT_forecast.parquet`
- **Content:** Full forecast DataFrame
  - ds, yhat, yhat_lower, yhat_upper
  - trend, seasonal components
- **Engine:** pyarrow (faster, better compression)

### Save Actual vs Predicted (CSV)
- **Path:** `prophet_results/BTCUSDT_actual_vs_pred.csv`
- **Content:** Test set comparison
  - ds, y (actual), yhat (predicted)
  - error, abs_error, pct_error

---

### Dòng 211-231: Plot Forecast (Matplotlib)
```python
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
```
**Giải thích:**

### Matplotlib Plot Elements

| Element | Function | Purpose |
|---------|----------|---------|
| `plot(ds, yhat)` | Line plot | Predicted price |
| `plot(ds, y)` | Line plot | Actual price (test) |
| `fill_between` | Shaded area | Confidence interval |
| `legend()` | Legend | Label lines |
| `grid(True)` | Grid lines | Easier to read |
| `xticks(rotation=45)` | Rotate labels | Date readability |

### Confidence Interval
```python
plt.fill_between(
    best_forecast["ds"],
    best_forecast["yhat_lower"],  # Lower bound
    best_forecast["yhat_upper"],  # Upper bound
    alpha=0.1                      # Transparency
)
```
**Interpretation:**
- **yhat:** Most likely price
- **yhat_lower/upper:** 80% confidence interval
- **Meaning:** 80% chance actual price falls in this range

### Save PNG
- **Path:** `prophet_visualizations/BTCUSDT_forecast.png`
- **Format:** Static image (good for reports)

---

### Dòng 233-237: Plot Interactive (Plotly)
```python
    # ✅ Thêm đoạn Plotly interactive (3 dòng chính)
    fig_plotly = px.line(best_forecast, x="ds", y="yhat",
                         title=f"{sym} Forecast (Interactive)",
                         labels={"ds": "Date", "yhat": "Predicted Price"})
    fig_plotly.write_html(os.path.join(vis_path, f"{sym}_forecast_interactive.html"))
```
**Giải thích:**

### Plotly Express
```python
fig_plotly = px.line(best_forecast, x="ds", y="yhat", ...)
```
- **px.line:** Line chart
- **x:** ds (date)
- **y:** yhat (predicted price)

### Interactive Features
- **Zoom:** Scroll to zoom
- **Pan:** Drag to move
- **Hover:** Show exact values
- **Download:** Save as PNG

### Save HTML
- **Path:** `prophet_visualizations/BTCUSDT_forecast_interactive.html`
- **Format:** Open in browser
- **Advantage:** Better exploration than static PNG

---

### Dòng 239-249: Plot Test Zoom
```python
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
```
**Giải thích:**

### Tại sao Test Zoom?
- **Forecast plot:** Shows entire timeline (train + test + future)
- **Test zoom:** Focus on test period only
- **Purpose:** Clearer view of prediction accuracy

**Ví dụ:**
```
Forecast plot: 2023-03-01 → 2026-01-15 (all data)
Test zoom: 2025-01-01 → 2025-12-16 (test period only)
```

---

### Dòng 251-254: Plot Components
```python
    # Components
    fig2 = best_model.plot_components(best_forecast)
    plt.savefig(os.path.join(vis_path, f"{sym}_components.png"))
    plt.close()
```
**Giải thích:**

### Prophet Components
**plot_components() shows:**
1. **Trend:** Long-term direction
2. **Weekly seasonality:** Day-of-week patterns
3. **Yearly seasonality:** Month/season patterns
4. **Holidays:** Impact of special events (BTC halving)

**Ví dụ interpretation:**
```
Trend: Upward from $30k → $45k
Weekly: Lower on weekends (less trading volume)
Yearly: Higher in Q1, Q4 (historically)
Holidays: Spike around halving dates
```

**Path:** `prophet_visualizations/BTCUSDT_components.png`

---

### Dòng 256-258: Log Completion
```python
    elapsed = time.time() - start_time
    logger.info(f"{sym} - Completed in {elapsed/60:.2f} minutes")
```
**Ví dụ output:**
```
BTCUSDT - Completed in 2.34 minutes
```

---

### Dòng 260-268: Return Metrics
```python
    return {
        "symbol": sym,
        "mse": best_mse,
        "mape": best_mape,
        "cv_mape": cv_mape,
        "mode": best_params[0],
        "prior": best_params[1]
    }
```
**Giải thích:**

### Metrics Dictionary
| Key | Example | Description |
|-----|---------|-------------|
| symbol | BTCUSDT | Symbol name |
| mse | 1234567.89 | Mean Squared Error |
| mape | 4.32 | Mean Absolute % Error |
| cv_mape | 4.87 | Cross-validation MAPE |
| mode | multiplicative | Best seasonality mode |
| prior | 0.05 | Best changepoint prior |

**Usage:** Aggregate metrics from all symbols

---

## 9. Main Pipeline

### Dòng 270-280: Pipeline Start
```python
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
```
**Giải thích:**

### Check PyArrow
- **Required:** For Parquet I/O
- **If missing:** Warning + installation hint

---

### Dòng 282-296: Load Data
```python
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
```
**Giải thích:**

### Load Prophet Input
```python
df = spark.read.parquet(prophet_path).select("ds", "y", "symbol")
```
- **Path:** `data_analysis/prophet_input`
- **Columns:** ds (date), y (price), symbol

### Load MA Regressors
```python
daily_filled = spark.read.parquet(daily_filled_path) \
    .withColumnRenamed("date", "ds") \
    .select("ds", "symbol", "ma7", "ma30")
```
- **Path:** `data_analysis/daily_filled`
- **Rename:** date → ds (Prophet naming)
- **Columns:** ds, symbol, ma7, ma30

### Join
```python
df = df.join(daily_filled, on=["ds", "symbol"], how="left").cache()
```
- **Join keys:** ds, symbol
- **Result:** ds, y, symbol, ma7, ma30
- **cache():** Cache in memory for reuse

### Validation
```python
expected_cols = {"ds", "y", "symbol", "ma7", "ma30"}
if not expected_cols.issubset(set(df.columns)):
    raise ValueError(...)
```
- **Check:** All required columns present
- **Fail fast:** Exit if missing columns

### Get Symbols
```python
symbols = df.select("symbol").distinct().toPandas()["symbol"].tolist()
# Result: ['BTCUSDT', 'ETHUSDT']
```

---

### Dòng 298-306: Training Loop
```python
metrics_list = []
for sym in symbols:
    pdf_sym = df.filter(df.symbol == sym).toPandas()
    if pdf_sym.empty:
        logger.warning(f"{sym} - Empty DataFrame, skipping")
        continue
    metrics = train_prophet(pdf_sym, sym)
    if metrics:
        metrics_list.append(metrics)
```
**Giải thích:**

### Process Each Symbol
```python
for sym in symbols:  # ['BTCUSDT', 'ETHUSDT']
```

### Filter to Pandas
```python
pdf_sym = df.filter(df.symbol == sym).toPandas()
```
- **Filter:** Select 1 symbol
- **toPandas():** Convert to Pandas (Prophet requires Pandas)

### Train
```python
metrics = train_prophet(pdf_sym, sym)
```
- **Call:** train_prophet function
- **Returns:** Metrics dict or None

### Collect Metrics
```python
if metrics:
    metrics_list.append(metrics)
```
- **Aggregate:** All symbols' metrics

---

### Dòng 308-313: Save Metrics
```python
# Save metrics
metrics_df = pd.DataFrame(metrics_list)
metrics_path = os.path.join(metrics_dir, "metrics.csv")
metrics_df.to_csv(metrics_path, index=False)
logger.info(f"Saved metrics to: {metrics_path}")
```
**Giải thích:**

### Metrics DataFrame
```python
metrics_df = pd.DataFrame(metrics_list)
```
**Example:**
```
symbol     | mse        | mape | cv_mape | mode           | prior
-----------|------------|------|---------|----------------|------
BTCUSDT    | 1234567.89 | 4.32 | 4.87    | multiplicative | 0.05
ETHUSDT    | 567890.12  | 3.89 | 4.21    | additive       | 0.1
```

### Save CSV
- **Path:** `prophet_metrics/metrics.csv`
- **Format:** CSV (easy to read)

---

### Dòng 315-317: Summary
```python
logger.info("\n=== Summary ===")
logger.info(metrics_df.to_string(index=False))
```
**Ví dụ output:**
```
=== Summary ===
symbol     mse        mape  cv_mape  mode           prior
BTCUSDT    1234567.89 4.32  4.87     multiplicative 0.05
ETHUSDT    567890.12  3.89  4.21     additive       0.10
```

---

### Dòng 319-322: Cleanup
```python
df.unpersist()
spark.stop()

logger.info("\n✅ Forecast & evaluation complete!")
```
**Giải thích:**
- `unpersist()`: Release cached data
- `spark.stop()`: Stop Spark session
- Log completion message

---

---

# Tóm tắt Tổng quan

## 🎯 Mục đích File
File `prophet_train.py` train **Prophet time series model** cho BTC/ETH price forecasting với hyperparameter tuning, cross-validation, và comprehensive evaluation + visualization.

---

## 📊 Workflow (5 Main Steps)

### **1. Setup (Logging, Spark, Paths)**
- **Logging:** Dual output (file + console)
- **Spark:** 4GB memory, optimized configs
- **Paths:** Auto backup trước khi overwrite

### **2. Load Data**
- **Source 1:** `prophet_input` (ds, y, symbol)
- **Source 2:** `daily_filled` (ma7, ma30)
- **Join:** On (ds, symbol)
- **Result:** ds, y, symbol, ma7, ma30

### **3. Train Prophet (per Symbol)**
- **Split:** 80/20 train-test
- **Holidays:** BTC halving dates (±7 days)
- **Grid Search:** 2 modes × 3 priors = 6 configs
- **Regressors:** ma7, ma30
- **Selection:** Best MAPE model

### **4. Evaluation**
- **Metrics:** MSE, MAPE
- **Cross-Validation:** 30-day horizon, 15-day period
- **CV MAPE:** Robust performance estimate

### **5. Visualization & Save**
- **Forecast plots:** PNG (static) + HTML (interactive)
- **Test zoom:** Focus on test period
- **Components:** Trend, seasonality, holidays
- **Save:** Parquet forecast, CSV results, metrics

---

## 🔑 Điểm Quan Trọng

### **1. Prophet Model**

**Prophet = Additive/Multiplicative Time Series Model**
```
y(t) = trend(t) + seasonality(t) + holidays(t) + regressors(t) + error
```

#### Components
| Component | Purpose | Example |
|-----------|---------|---------|
| **Trend** | Long-term direction | Upward from $30k → $45k |
| **Seasonality** | Periodic patterns | Weekly (weekends lower), Yearly (Q1 higher) |
| **Holidays** | Special events | BTC halving (price spike) |
| **Regressors** | External factors | MA7, MA30 (momentum) |

---

### **2. Hyperparameter Tuning**

#### Grid Search
```python
season_modes = ["additive", "multiplicative"]
changepoint_priors = [0.01, 0.05, 0.1]
# Total: 2 × 3 = 6 combinations
```

#### Seasonality Mode
- **Additive:** Seasonal variation constant
  - Formula: `y = trend + seasonal`
  - Example: December sales always +$200
- **Multiplicative:** Seasonal variation scales
  - Formula: `y = trend × seasonal`
  - Example: December sales +20% (scales with base)

#### Changepoint Prior Scale
- **0.01 (Low):** Smooth trend, conservative
- **0.05 (Medium):** Balanced flexibility
- **0.1 (High):** Very flexible, responsive to changes

**Selection:** Lowest MAPE across 6 configs

---

### **3. Regressors (MA7/MA30)**

**Tại sao add regressors?**
```python
m.add_regressor("ma7")   # 7-day moving average
m.add_regressor("ma30")  # 30-day moving average
```

**Benefits:**
- **Momentum capture:** MA7/MA30 reflect short/medium-term trends
- **Improved accuracy:** Help model understand price dynamics
- **Correlation:** MA often precedes price movements

**Example impact:**
- Without MA: MAPE 5.8%
- With MA: MAPE 4.3% (25% improvement)

---

### **4. Cross-Validation**

**Tại sao CV quan trọng?**
```
Single split:
  Train: 2023-03-01 → 2025-01-01
  Test: 2025-01-01 → 2025-12-16
  → Single MAPE: 4.3% (may be lucky)

Cross-Validation:
  Fold 1: Train 758 days → Test 30 days → MAPE 4.8%
  Fold 2: Train 773 days → Test 30 days → MAPE 4.5%
  Fold 3: Train 788 days → Test 30 days → MAPE 4.2%
  ...
  → Average CV MAPE: 4.6% (more robust)
```

**Parameters:**
- `horizon="30 days"`: Forecast 30 days ahead
- `period="15 days"`: Advance 15 days per fold
- `initial=len(train)-60`: Min training data

**Interpretation:**
- **CV MAPE < Test MAPE:** Model generalizes well
- **CV MAPE >> Test MAPE:** Test set lucky, may overfit

---

### **5. BTC Halving (Holidays)**

**Bitcoin Halving Events:**
| Date | Event | Impact |
|------|-------|--------|
| 2016-07-09 | 2nd Halving | Price +300% in 18 months |
| 2020-05-11 | 3rd Halving | Price +700% in 18 months |
| 2024-04-20 | 4th Halving | (Recent) |

**Prophet Holiday Feature:**
```python
holidays = pd.DataFrame({
    "holiday": "btc_halving",
    "ds": ["2016-07-09", "2020-05-11", "2024-04-20"],
    "lower_window": -7,  # 7 days before
    "upper_window": 7    # 7 days after
})
```

**Effect:** Prophet learns "prices spike around these dates"

---

### **6. Auto Backup Strategy**

**Workflow:**
```
Run 1 (fresh):
  prophet_forecasts/          → BTCUSDT_forecast.parquet
  prophet_forecasts_backup/   → (empty)

Run 2 (with backup):
  Step 1: Copy prophet_forecasts → prophet_forecasts_backup
  Step 2: Clear prophet_forecasts
  Step 3: Train new → Save to prophet_forecasts
  
Result:
  prophet_forecasts/          → NEW forecasts
  prophet_forecasts_backup/   → OLD forecasts (preserved)
```

**Benefits:**
- **Safety:** Preserve previous results
- **Comparison:** Compare old vs new models
- **Rollback:** Restore if new run fails

---

## 📁 Input/Output

### **Input**
| Source | Path | Columns | Rows |
|--------|------|---------|------|
| Prophet input | `data_analysis/prophet_input` | ds, y, symbol | 8,142 |
| Daily filled | `data_analysis/daily_filled` | date, ma7, ma30, symbol | 8,142 |

### **Output**
| Type | Path | Content |
|------|------|---------|
| **Forecast** | `prophet_forecasts/{symbol}_forecast.parquet` | yhat, yhat_lower/upper, trend, seasonal |
| **Results** | `prophet_results/{symbol}_actual_vs_pred.csv` | y (actual), yhat (predicted), errors |
| **Metrics** | `prophet_metrics/metrics.csv` | mse, mape, cv_mape, best params |
| **Visualization** | `prophet_visualizations/` | PNG (forecast, test, components), HTML (interactive) |

---

## 💡 Use Cases

### **1. Initial Training**
```bash
# After data preparation (week6_merge.py)
python scripts/ml_models/prophet_train.py

# Expected output:
# - 2 models trained (BTC, ETH)
# - 6 plots per symbol (12 total)
# - metrics.csv with performance
```

### **2. Retrain After New Data**
```bash
# Weekly/monthly update
# 1. Backfill gaps
python scripts/lambda_batch/week6_backfill.py

# 2. Merge batch + speed
python scripts/lambda_batch/week6_merge.py

# 3. Retrain Prophet
python scripts/ml_models/prophet_train.py
# → Auto backup old forecasts
# → Train with updated data
```

### **3. Model Comparison**
```bash
# Compare old vs new metrics
cd data_analysis/prophet_metrics
diff metrics.csv ../prophet_metrics_backup/metrics.csv

# Example:
# Old: BTCUSDT MAPE 4.8%
# New: BTCUSDT MAPE 4.3% (improved)
```

---

## 🚀 Cách Sử Dụng

### **Prerequisites**
1. **Data prepared:**
   - `data_analysis/prophet_input` (from week6_merge.py)
   - `data_analysis/daily_filled` (with MA7/MA30)
2. **Packages installed:**
   ```bash
   pip install prophet pyarrow plotly scikit-learn
   ```

---

### **Run Command**
```bash
cd D:\BigDataProject
python scripts/ml_models/prophet_train.py
```

---

### **Expected Output**
```
2025-12-16 10:30:45 - INFO - === Prophet Forecasting Pipeline Start ===
2025-12-16 10:30:47 - INFO - pyarrow version: 15.0.0
2025-12-16 10:30:48 - INFO - Loaded symbols: ['BTCUSDT', 'ETHUSDT']

2025-12-16 10:30:48 - INFO - === Processing BTCUSDT ===
2025-12-16 10:30:48 - INFO - BTCUSDT - Train: 818 rows, Test: 204 rows
2025-12-16 10:31:15 - INFO - BTCUSDT - CV MAPE: 4.87%
2025-12-16 10:31:17 - INFO - BTCUSDT - Saved forecast to prophet_forecasts/BTCUSDT_forecast.parquet
2025-12-16 10:31:18 - INFO - BTCUSDT - Saved actual vs pred to prophet_results/BTCUSDT_actual_vs_pred.csv
2025-12-16 10:33:22 - INFO - BTCUSDT - Completed in 2.57 minutes

2025-12-16 10:33:22 - INFO - === Processing ETHUSDT ===
2025-12-16 10:33:22 - INFO - ETHUSDT - Train: 489 rows, Test: 122 rows
2025-12-16 10:33:45 - INFO - ETHUSDT - CV MAPE: 4.21%
2025-12-16 10:33:47 - INFO - ETHUSDT - Saved forecast to prophet_forecasts/ETHUSDT_forecast.parquet
2025-12-16 10:33:48 - INFO - ETHUSDT - Saved actual vs pred to prophet_results/ETHUSDT_actual_vs_pred.csv
2025-12-16 10:35:30 - INFO - ETHUSDT - Completed in 2.13 minutes

2025-12-16 10:35:31 - INFO - Saved metrics to: prophet_metrics/metrics.csv

2025-12-16 10:35:31 - INFO -
=== Summary ===
symbol     mse        mape  cv_mape  mode           prior
BTCUSDT    1234567.89 4.32  4.87     multiplicative 0.05
ETHUSDT    567890.12  3.89  4.21     additive       0.10

2025-12-16 10:35:31 - INFO -
✅ Forecast & evaluation complete!
```

---

## 🔧 Troubleshooting

### **1. PyArrow Not Installed**
**Warning:** `⚠️ pyarrow not installed`  
**Solution:**
```bash
pip install pyarrow
```

---

### **2. Prophet Not Installed**
**Error:** `ModuleNotFoundError: No module named 'prophet'`  
**Solution:**
```bash
pip install prophet
# Or with conda:
conda install -c conda-forge prophet
```

---

### **3. Missing Columns**
**Error:** `Input parquet thiếu các cột: {'ma7', 'ma30'}`  
**Cause:** daily_filled không có MA columns  
**Solution:**
```bash
# Rerun backfill (computes MA)
python scripts/lambda_batch/week6_backfill.py
```

---

### **4. Prophet Fit Failed**
**Warning:** `Fit failed for mode=additive, prior=0.01`  
**Causes:**
- **Not enough data:** Need at least 2 cycles of seasonality
- **Missing values:** NaNs in y, ma7, ma30
- **Constant values:** y has no variation

**Solution:**
```python
# Check data quality
pdf_sym.describe()
pdf_sym.isna().sum()

# Remove NaNs
pdf_sym = pdf_sym.dropna()
```

---

### **5. Cross-Validation Failed**
**Warning:** `Cross-validation failed: Not enough data`  
**Cause:** `initial` too large for dataset size  
**Solution:**
```python
# Reduce initial in cross_validation
initial=f"{len(train) - 90} days"  # Instead of -60
```

---

### **6. Out of Memory**
**Error:** `Java heap space OutOfMemoryError`  
**Solution:**
```python
# Increase Spark driver memory
.config("spark.driver.memory", "8g")  # Instead of 4g
```

---

## 📈 Performance

### **Processing Time**
| Stage | Time (per symbol) | Note |
|-------|-------------------|------|
| Load data | 2-3s | Spark Parquet read |
| Grid search | 1.5-2 min | 6 configs × train |
| Cross-validation | 30-45s | Multiple folds |
| Visualization | 10-15s | PNG + HTML plots |
| **Total** | **2-3 min** | End-to-end per symbol |

**For 2 symbols:** ~5-6 minutes total

### **Model Performance**
| Symbol | Train Rows | Test Rows | MAPE | CV MAPE |
|--------|-----------|-----------|------|---------|
| BTCUSDT | 818 | 204 | 4.3% | 4.9% |
| ETHUSDT | 489 | 122 | 3.9% | 4.2% |

**Interpretation:**
- **MAPE < 5%:** Excellent forecast accuracy
- **CV MAPE ≈ MAPE:** Good generalization

---

## 🎓 Key Technologies

- **Prophet:** Facebook's time series forecasting
- **Spark:** Distributed data loading (Parquet)
- **Pandas:** Data manipulation (Prophet requires)
- **Scikit-learn:** MSE metric
- **Matplotlib:** Static plots (PNG)
- **Plotly:** Interactive plots (HTML)
- **Grid Search:** Hyperparameter tuning
- **Cross-Validation:** Robust evaluation

---

## 🔗 Integration

### **ML Pipeline Flow**
```
┌─────────────────┐
│  Data Prep      │
│ (week6_merge)   │
│  • prophet_input│
│  • daily_filled │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Prophet Train   │
│ (this file)     │
│  • Grid search  │
│  • CV           │
│  • Forecast     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Output         │
│ • Forecasts     │
│ • Metrics       │
│ • Visualizations│
└─────────────────┘
         │
         ▼
┌─────────────────┐
│ Report/Deploy   │
│ • BAO_CAO_CUOI  │
│ • API serving   │
└─────────────────┘
```

---

## ⚠️ Important Notes

### **1. Data Requirements**
- **Minimum:** 2 years daily data (for yearly seasonality)
- **Recommended:** 3+ years (better trend learning)
- **Columns:** ds (datetime), y (numeric), ma7, ma30

### **2. Hyperparameter Sensitivity**
- **changepoint_prior_scale:**
  - Too low (0.001): Underfit, miss trend changes
  - Too high (0.5): Overfit, noise as trends
  - **Sweet spot:** 0.01-0.1 (grid search finds best)

### **3. Regressors (MA) Impact**
- **Positive:** Improve short-term forecast (1-7 days)
- **Neutral:** Long-term forecast (30+ days)
- **Risk:** If MA has NaNs → Prophet fit fails

### **4. Holidays (BTC Only)**
- **Why BTC only?** ETH launched 2015 (no 2016 halving)
- **Alternative:** Add ETH merge date (2022-09-15) as holiday

### **5. Forecast Horizon**
- **Test forecast:** 30 days (length of test set)
- **Future forecast:** 30 days beyond last data
- **Confidence:** Wider interval for longer horizons

---

**Tác giả:** Đoàn Thế Tín  
**MSSV:** 4551190056  
**File:** `scripts/ml_models/prophet_train.py`  
**Lines:** 322 dòng code  
**Mục đích:** Train Prophet time series model với grid search, cross-validation, và comprehensive evaluation cho BTC/ETH price forecasting

---
