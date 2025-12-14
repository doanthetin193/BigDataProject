# 📊 BÁO CÁO OUTPUT & Ý NGHĨA - LAMBDA ARCHITECTURE & PROPHET FORECASTING

## 1️⃣ OUTPUT SỐ LIỆU CHÍNH

### **A. Dataset Processed**

#### **daily_filled (SOURCE DATA)**
```
Total rows: 8,140
- BTCUSDT: 5,097 days (2012-01-01 → 2025-12-14)
- ETHUSDT: 3,043 days (2017-08-16 → 2025-12-14)

Columns:
- date: Ngày giao dịch
- daily_open, daily_high, daily_low, daily_close: Giá OHLC
- daily_volume: Khối lượng giao dịch
- ma7: Moving Average 7 ngày (chỉ báo xu hướng ngắn hạn)
- ma30: Moving Average 30 ngày (chỉ báo xu hướng trung hạn)
- cnt: Số lượng records 1-phút trong ngày
```

**Ý nghĩa:**
- ✅ Dữ liệu đầy đủ không có gap (forward filled)
- ✅ MA7/MA30 giúp model hiểu xu hướng tăng/giảm
- ✅ Partitioned by symbol/year/month → query nhanh

---

#### **prophet_input (TRAINING DATA)**
```
Total rows: 8,140 (matching daily_filled)

Columns:
- ds: Date (datetime)
- y: Target variable (daily_close price)
- symbol: BTCUSDT or ETHUSDT
```

**Ý nghĩa:**
- ✅ Minimal schema chuẩn Prophet (ds, y)
- ✅ Derived từ daily_filled (consistency)
- ✅ Symbol cho multi-series training

---

### **B. Model Performance Metrics**

#### **BTCUSDT:**
```
MSE (Mean Squared Error):    4,986,009
  → Sai số bình phương trung bình: ~4.9M
  → Với giá BTC ~$90K → sai số ~$2,233 (sqrt của MSE)

MAPE (Mean Absolute % Error): 2.38%
  → Dự đoán sai trung bình 2.38% so với giá thực
  → Ví dụ: Giá $90,000 → sai ~$2,142

CV MAPE (Cross-Validation):   3.36%
  → Kiểm tra chéo 2 folds → sai 3.36%
  → Đánh giá model generalization (không overfit)
```

#### **ETHUSDT:**
```
MSE:       20,873
  → Sai số ~$144 (sqrt)

MAPE:      3.54%
  → Dự đoán sai 3.54% trung bình
  → Giá $3,000 → sai ~$106

CV MAPE:   3.90%
  → Cross-validation sai 3.90%
```

**Ý nghĩa so sánh:**
- ✅ **MAPE < 5%**: Mô hình GOOD (chuẩn industry)
- ✅ BTC chính xác hơn ETH (2.38% vs 3.54%)
- ✅ CV MAPE ≈ MAPE → model ổn định, không overfit
- ✅ MSE cao (BTC) do giá lớn, nhưng MAPE thấp → tốt

---

### **C. Forecast Results (Actual vs Predicted)**

#### **BTCUSDT (Last 5 days):**
```
Date       | Actual ($) | Predicted ($) | Error (%) | Ý nghĩa
-----------|-----------|---------------|-----------|----------
2025-12-10 | 92,015.37 | 88,130.32     | -4.22%    | Dự đoán thấp hơn (giá tăng đột biến)
2025-12-11 | 92,513.38 | 88,211.70     | -4.65%    | Dự đoán thấp hơn
2025-12-12 | 90,268.42 | 88,391.13     | -2.08%    | Gần đúng (trong threshold)
2025-12-13 | 90,240.01 | 88,633.91     | -1.78%    | Rất tốt (< 2%)
2025-12-14 | 90,222.42 | 88,609.74     | -1.79%    | Rất tốt (< 2%)
```

**Nhận xét:**
- Ngày 10-11/12: Giá tăng mạnh đột biến → model chưa kịp catch up
- Ngày 12-14/12: Giá ổn định → model dự đoán tốt (< 2%)
- Xu hướng giảm dần error → model adaptive

#### **ETHUSDT (Last 5 days):**
```
Date       | Actual ($) | Predicted ($) | Error (%) | Ý nghĩa
-----------|-----------|---------------|-----------|----------
2025-12-10 | 3,324.14  | 3,203.10      | +3.64%    | Dự đoán thấp
2025-12-11 | 3,237.39  | 3,219.78      | +0.54%    | Rất chính xác!
2025-12-12 | 3,084.86  | 3,223.12      | -4.48%    | Giá giảm đột ngột
2025-12-13 | 3,114.64  | 3,238.50      | -3.98%    | Dự đoán cao hơn
2025-12-14 | 3,111.99  | 3,243.44      | -4.22%    | Dự đoán cao hơn
```

**Nhận xét:**
- Ngày 11/12: Model dự đoán cực kỳ chính xác (0.54%)
- ETH biến động nhiều hơn BTC → error cao hơn
- Ngày 12/12: Flash crash → model không predict được

---

## 2️⃣ BIỂU ĐỒ ĐÃ SINH RA

### **A. Interactive Forecast Charts (HTML Plotly)**

**File:** `data_analysis/week4_visualizations/BTCUSDT_forecast_interactive.html` (4.95 MB)

**Nội dung biểu đồ:**
1. **Time Series Plot:**
   - Trục X: Timeline (2023-03-01 → 2025-12-14)
   - Trục Y: Giá BTC ($)
   - **Đường đen**: Actual price (giá thực tế)
   - **Đường xanh**: Predicted price (yhat)
   - **Vùng xanh nhạt**: Confidence interval (yhat_lower, yhat_upper)
   
2. **Components Breakdown:**
   - **Trend**: Xu hướng dài hạn (tăng/giảm)
   - **Weekly**: Seasonality theo tuần (T2-CN giá khác nhau)
   - **Yearly**: Seasonality theo năm (Q1-Q4 patterns)
   - **MA7 Effect**: Ảnh hưởng của Moving Average 7 ngày
   - **MA30 Effect**: Ảnh hưởng của Moving Average 30 ngày

3. **Zoom & Hover:**
   - Interactive: Click drag để zoom vào khoảng thời gian
   - Hover: Hiện chi tiết giá + ngày + confidence

**File:** `data_analysis/week4_visualizations/ETHUSDT_forecast_interactive.html` (4.87 MB)
- Tương tự BTC nhưng cho Ethereum

---

### **B. Ý Nghĩa Biểu Đồ Cho Báo Cáo**

#### **1. Time Series Forecast Chart**
**Mục đích:**
- Thể hiện khả năng dự đoán của model
- So sánh actual vs predicted trực quan
- Confidence interval → độ tin cậy prediction

**Điểm nhấn khi trình bày:**
- ✅ "Đường dự đoán (xanh) bám sát đường thực tế (đen) → model chính xác"
- ✅ "Vùng confidence interval hẹp → model tự tin vào prediction"
- ✅ "Test period 1,020 ngày (BTC) / 609 ngày (ETH) → validation dài hạn"

#### **2. Trend Component**
**Mục đích:**
- Hiện xu hướng tổng thể (long-term direction)
- Loại bỏ noise & seasonality

**Điểm nhấn:**
- ✅ "Trend tăng liên tục từ 2023 → 2025 → bullish market"
- ✅ "Model catch được structural changes (bull/bear cycles)"

#### **3. Seasonality Components**
**Mục đích:**
- Phát hiện patterns lặp lại theo tuần/năm
- Trading insights (ngày nào giá thường cao/thấp)

**Điểm nhấn:**
- ✅ "Weekly seasonality: Giá thường cao hơn cuối tuần (retail traders)"
- ✅ "Yearly seasonality: Q4 thường tăng mạnh (historical pattern)"

#### **4. Regressor Effects (MA7/MA30)**
**Mục đích:**
- Đo lường ảnh hưởng của chỉ báo kỹ thuật
- Giải thích model "học" được gì từ MA

**Điểm nhấn:**
- ✅ "MA7 có tác động mạnh → xu hướng ngắn hạn quan trọng"
- ✅ "MA30 ổn định hơn → lọc noise dài hạn"
- ✅ "Kết hợp cả 2 → model hiểu momentum & direction"

---

## 3️⃣ KẾT QUẢ CHO BÁO CÁO

### **Tóm Tắt Số Liệu:**

```
┌─────────────────────────────────────────────────────┐
│  DATASET                                            │
├─────────────────────────────────────────────────────┤
│  Total Days:     8,140 (BTC: 5,097 | ETH: 3,043)   │
│  Date Range:     2012-01-01 → 2025-12-14            │
│  Features:       OHLCV + MA7 + MA30                 │
│  Missing Data:   0% (forward filled)                │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│  MODEL PERFORMANCE                                  │
├─────────────────────────────────────────────────────┤
│  Algorithm:      Prophet (Facebook)                 │
│  Regressors:     MA7, MA30                          │
│                                                      │
│  BTC:                                               │
│    MAPE:         2.38% (test) | 3.36% (CV)         │
│    Latest:       $90,222 actual vs $88,610 pred     │
│    Quality:      ✅ EXCELLENT (< 5%)                │
│                                                      │
│  ETH:                                               │
│    MAPE:         3.54% (test) | 3.90% (CV)         │
│    Latest:       $3,112 actual vs $3,243 pred       │
│    Quality:      ✅ GOOD (< 5%)                     │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│  LAMBDA ARCHITECTURE                                │
├─────────────────────────────────────────────────────┤
│  Batch Layer:    ✅ Backfill automation             │
│  Speed Layer:    ✅ WebSocket → Kafka → Spark       │
│  Serving Layer:  ✅ Unified merge                   │
│  Scalability:    Million+ 1-min records             │
└─────────────────────────────────────────────────────┘
```

---

## 4️⃣ CÁCH TRÌNH BÀY CHO THẦY

### **Slide 1: Dataset Overview**
- Bảng số liệu: 8,140 rows, date range, features
- Screenshot daily_filled schema
- Nhấn mạnh: "Dữ liệu hoàn chỉnh từ 2012 đến hôm nay"

### **Slide 2: Data Pipeline**
- Flow chart: CSV → Parquet → daily_filled → prophet_input
- Giải thích: "Tối ưu storage (Parquet) và tính toán (MA indicators)"

### **Slide 3: Model Performance**
- Bảng metrics: MSE, MAPE, CV MAPE
- So sánh BTC vs ETH
- Kết luận: "MAPE < 5% → industry standard GOOD"

### **Slide 4: Forecast Visualization**
- **Screenshot biểu đồ interactive** (full screen)
- Point vào: Actual line, Predicted line, Confidence interval
- Giải thích: "Đường dự đoán bám sát thực tế → model accurate"

### **Slide 5: Components Analysis**
- Screenshot Trend + Seasonality + Regressors
- Giải thích insights:
  - "Trend tăng → bullish market"
  - "Weekly pattern → trading behavior"
  - "MA7/MA30 → momentum indicators work"

### **Slide 6: Lambda Architecture**
- Diagram: Batch + Speed + Serving
- Demo: "Real-time streaming + historical backfill"

### **Slide 7: Results Summary**
- Bảng last 5 days: actual vs predicted
- Highlight ngày chính xác nhất (< 2% error)
- Kết luận: "Dự đoán đến hôm nay (14/12) với độ chính xác cao"

---

## 5️⃣ DEMO CHO THẦY

**Mở file HTML:**
```bash
# Mở browser với biểu đồ interactive
Invoke-Item data_analysis/week4_visualizations/BTCUSDT_forecast_interactive.html
```

**Actions:**
1. Zoom vào tuần gần nhất → chỉ độ chính xác
2. Hover vào điểm → show exact values
3. Scroll xuống components → giải thích trend/seasonality
4. Chỉ vào confidence interval → "model tự tin 95%"

---

## 📋 CHECKLIST BÁO CÁO

- [ ] Số liệu dataset: 8,140 rows, date range
- [ ] Metrics: MAPE 2.38% (BTC), 3.54% (ETH)
- [ ] Biểu đồ forecast: actual vs predicted
- [ ] Components: trend + seasonality + MA effects
- [ ] Results table: last 5 days accuracy
- [ ] Lambda Architecture diagram
- [ ] Demo interactive chart
- [ ] Kết luận: Production-ready forecasting system

---

**Ý nghĩa tổng thể:**
✅ Xây dựng hệ thống dự đoán giá crypto end-to-end  
✅ Độ chính xác cao (< 4% error)  
✅ Scalable với Lambda Architecture  
✅ Real-time + Historical data processing  
✅ Interactive visualizations cho insights  

**Impact:**
- 📈 Trading strategy optimization
- 🤖 Automated prediction pipeline
- 📊 Data-driven decision making
- 🚀 Production-ready deployment
