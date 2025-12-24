# 🚀 Streamlit Dashboard - Dự đoán Giá Tiền Mã Hóa

## Khởi động Nhanh

### 1. Cài đặt Dependencies
```bash
pip install -r requirements_web.txt
```

### 2. Chạy Dashboard
```bash
streamlit run app.py
```

Dashboard sẽ mở trên trình duyệt tại: **http://localhost:8501**

---

## 📋 Tính năng

### Trang chủ (app.py)
- Tổng quan đồ án
- Thống kê nhanh
- Giới thiệu Lambda Architecture
- Hướng dẫn điều hướng

### 📊 Trang Metrics
- Bảng hiệu suất model (MAPE, MSE, CV MAPE)
- Biểu đồ so sánh MAPE
- Cross-validation vs Test MAPE
- Hyperparameters tốt nhất
- Tải xuống metrics CSV

### 📈 Trang Forecasts
- Chọn symbol (BTC/ETH)
- Biểu đồ tương tác Thực tế vs Dự đoán
- Histogram phân phối sai số
- Sai số theo thời gian
- Bảng dự đoán gần đây
- Tải xuống predictions CSV

### 📁 Trang Data Info
- Thống kê dataset Daily Filled
- Thống kê dataset Daily Raw
- Schema Prophet Input
- Dữ liệu mẫu
- Giải thích pipeline

---

## 🎨 Giao diện

**Trang chủ:**
```
┌─────────────────────────────────────────┐
│  🚀 Dashboard Dự đoán Giá Tiền Mã Hóa   │
│                                         │
│  Metrics  |  Forecasts  |  Data Info   │
│                                         │
│  Thống kê Nhanh:                        │
│  Symbols: 2  | MAPE TB: 2.96%          │
│  Model tốt nhất: BTCUSDT                │
│                                         │
│  Tổng quan Đồ án...                     │
└─────────────────────────────────────────┘
```

---

## 📂 Cấu trúc File

```
BigDataProject/
├── app.py                          # Entry point chính
├── pages/
│   ├── 1_📊_Metrics.py            # Hiển thị metrics
│   ├── 2_📈_Forecasts.py          # Biểu đồ dự đoán
│   └── 3_📁_Data_Info.py          # Thông tin dataset
├── .streamlit/
│   └── config.toml                 # Config Streamlit
├── requirements_web.txt            # Web dependencies
└── README_STREAMLIT.md             # File này
```

---

## 🔧 Xử lý Lỗi

### Dashboard hiển thị "Data not found"
**Giải pháp:** Chạy các script xử lý dữ liệu trước:
```bash
python scripts/ml_models/prophet_train.py
```

### Lỗi Spark
**Giải pháp:** Kiểm tra thư mục `data_analysis/` có các file Parquet chưa

### Port đã được sử dụng
**Giải pháp:** Dùng port khác:
```bash
streamlit run app.py --server.port 8502
```

---

## 💡 Gợi ý

1. **Làm mới dữ liệu:** Sau khi chạy `prophet_train.py`, refresh trình duyệt
2. **Thêm symbols:** Cập nhật danh sách symbols trong code
3. **Tùy chỉnh theme:** Sửa file `.streamlit/config.toml`
4. **Deploy online:** Sử dụng Streamlit Cloud (miễn phí)

---

## 🎓 Cho Buổi Thuyết trình

**Flow demo:**
1. Mở dashboard: `streamlit run app.py`
2. Giới thiệu Trang chủ → Giải thích Lambda Architecture
3. Chuyển đến Metrics → Cho thấy MAPE < 5%
4. Chuyển đến Forecasts → Demo biểu đồ tương tác
5. Chuyển đến Data Info → Giải thích pipeline

**Điểm nhấn:**
- Giao diện tương tác (không phải file PNG tĩnh)
- Dữ liệu real-time từ Parquet
- UI chuyên nghiệp
- Dễ hiểu và trực quan

---

## 📚 Tài liệu Liên quan

- `BAO_CAO_CHINH_THUC.md` - Báo cáo đồ án chính thức
- `docs/FAQ_GIAI_THICH_BAO_CAO.md` - Giải thích các thuật ngữ
- `MERMAID_DIAGRAMS.md` - Sơ đồ cho báo cáo

---

**Tác giả:** Đoàn Thế Tín  
**Ngày:** 24/12/2025  
**Framework:** Streamlit 1.28+
