# 🚀 Streamlit Dashboard - Crypto Price Forecasting

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements_web.txt
```

### 2. Run Dashboard
```bash
streamlit run app.py
```

The dashboard will open in your browser at: **http://localhost:8501**

---

## 📋 Features

### Home Page (app.py)
- Project overview
- Quick statistics
- Lambda Architecture explanation
- Navigation guide

### 📊 Metrics Page
- Model performance table (MAPE, MSE, CV MAPE)
- MAPE comparison bar chart
- Cross-validation vs Test MAPE
- Hyperparameters display
- Download metrics CSV

### 📈 Forecasts Page
- Symbol selection (BTC/ETH)
- Interactive actual vs predicted chart
- Error distribution histogram
- Error over time line chart
- Recent predictions table
- Download predictions CSV

### 📁 Data Info Page
- Daily filled dataset statistics
- Daily raw dataset info
- Prophet input schema
- Sample data preview
- Data pipeline explanation

---

## 🎨 Screenshots

**Home:**
```
┌─────────────────────────────────────────┐
│  🚀 Cryptocurrency Price Forecasting    │
│                                         │
│  Metrics  |  Forecasts  |  Data Info   │
│                                         │
│  Quick Stats:                           │
│  Symbols: 2  | Avg MAPE: 2.96%         │
│  Best Model: BTCUSDT                    │
│                                         │
│  Project Overview...                    │
└─────────────────────────────────────────┘
```

---

## 📂 File Structure

```
BigDataProject/
├── app.py                          # Main entry point
├── pages/
│   ├── 1_📊_Metrics.py            # Metrics visualization
│   ├── 2_📈_Forecasts.py          # Forecast charts
│   └── 3_📁_Data_Info.py          # Dataset info
├── .streamlit/
│   └── config.toml                 # Streamlit config
├── requirements_web.txt            # Web dependencies
└── README_STREAMLIT.md             # This file
```

---

## 🔧 Troubleshooting

### Dashboard shows "Data not found"
**Solution:** Run data processing scripts first:
```bash
python scripts/ml_models/prophet_train.py
```

### Spark errors
**Solution:** Check if data_analysis/ folder exists and has Parquet files

### Port already in use
**Solution:** Use different port:
```bash
streamlit run app.py --server.port 8502
```

---

## 💡 Tips

1. **Refresh data:** After running prophet_train.py, refresh browser
2. **Multiple symbols:** Add more cryptocurrencies by updating symbols list
3. **Custom styling:** Edit `.streamlit/config.toml` for theme
4. **Deploy:** Use Streamlit Cloud (free) for online demo

---

## 🎓 For Presentation

**Demo flow:**
1. Open dashboard: `streamlit run app.py`
2. Show Home → Explain Lambda Architecture
3. Navigate to Metrics → Show MAPE < 5%
4. Navigate to Forecasts → Interactive chart demo
5. Navigate to Data Info → Explain pipeline

**Key points:**
- Interactive (not static PNG files)
- Real-time data from Parquet
- Professional UI
- Easy to understand

---

**Author:** Đoàn Thế Tín  
**Date:** 24/12/2025  
**Framework:** Streamlit 1.28+
