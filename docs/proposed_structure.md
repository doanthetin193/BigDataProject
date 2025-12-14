# Cấu trúc thư mục đề xuất

## Hiện tại (lộn xộn):
```
D:\BigDataProject/
├── convert_to_parquet.py
├── clean_parquet.py
├── preprocess_step1.py
├── preprocess_step2.py
├── prophet_train.py
├── week6_backfill.py
├── week6_backfill_batch.py
├── week6_merge.py
├── week6_merge_temp.py
├── check_data.py
├── check_forecast.py
├── final_verification.py
├── cleanup_analysis.py
├── BAO_CAO_CUOI_KY.md
├── README.md
├── PROJECT_SUMMARY.md
├── WEEK6_*.md (7 files)
├── data/
├── data_analysis/
└── week6_streaming/
```

## Đề xuất (có tổ chức):
```
D:\BigDataProject/
├── scripts/
│   ├── preprocessing/
│   │   ├── convert_to_parquet.py
│   │   ├── clean_parquet.py
│   │   ├── preprocess_step1.py
│   │   └── preprocess_step2.py
│   │
│   ├── lambda_batch/
│   │   ├── week6_backfill.py
│   │   ├── week6_backfill_batch.py
│   │   ├── week6_merge.py
│   │   └── week6_merge_temp.py
│   │
│   ├── ml_models/
│   │   └── prophet_train.py
│   │
│   └── utils/
│       ├── check_data.py
│       ├── check_forecast.py
│       ├── final_verification.py
│       └── cleanup_analysis.py
│
├── docs/
│   ├── BAO_CAO_CUOI_KY.md
│   ├── README.md
│   ├── PROJECT_SUMMARY.md
│   ├── WEEK6_01_TONG_QUAN.md
│   ├── WEEK6_02_BATCH_LAYER.md
│   ├── WEEK6_03_SPEED_LAYER.md
│   ├── WEEK6_04_SERVING_LAYER.md
│   ├── WEEK6_HUONG_DAN_CHAY.md
│   ├── WEEK6_LAMBDA_ARCHITECTURE.md
│   └── WEEK6_SPARK_STREAMING_CONSUMER_GIAI_THICH.md
│
├── data/                 (giữ nguyên)
├── data_analysis/        (giữ nguyên)
├── data_parquet/         (giữ nguyên)
├── hadoop/               (giữ nguyên)
├── logs/                 (giữ nguyên)
└── week6_streaming/      (giữ nguyên)
```

## Lợi ích:
- ✅ Dễ tìm file theo chức năng
- ✅ Tách biệt docs vs code
- ✅ Nhóm scripts theo workflow
- ✅ Utils/debug riêng biệt
