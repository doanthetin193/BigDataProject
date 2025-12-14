"""
Files cleanup recommendation
"""

print("="*80)
print("FILES CLEANUP ANALYSIS")
print("="*80)

# 1. Temporary/Debug files (CÓ THỂ XÓA)
temp_files = [
    "check_data.py",           # Debug script - created today
    "check_forecast.py",       # Debug script - created today  
    "final_verification.py",   # One-time verification script
]

# 2. Old backup folders (CÓ THỂ XÓA SAU KHI VERIFY)
old_folders = [
    "data_analysis/week4_forecasts_old/",
    "data_analysis/week4_metrics_old/",
    "data_analysis/week4_results_old/",
    "data_analysis/week4_visualizations_old/",
]

# 3. Temp merge file (CÓ THỂ XÓA)
temp_scripts = [
    "week6_merge_temp.py",     # Temporary script for one-time merge
]

# 4. Unused data folders (CÓ THỂ XÓA)
unused_folders = [
    "data_analysis/daily_raw_csv/",        # CSV copy of parquet
    "data_analysis/large_gaps_top100/",    # Analysis output
    "data_analysis/missing_days/",         # Empty folder
    "data_analysis/per_day_counts/",       # Analysis output
    "data_analysis/results/",              # Old analysis results
    "data_analysis/visualizations/",       # Old visualizations
]

# 5. Core files (GIỮ LẠI)
keep_files = [
    "convert_to_parquet.py",         # Convert CSV → Parquet
    "clean_parquet.py",              # Clean duplicates
    "preprocess_step1.py",           # daily_raw generation
    "preprocess_step2.py",           # daily_filled + prophet_input
    "prophet_train.py",              # Prophet training
    "week6_backfill.py",             # Batch layer (regular updates)
    "week6_backfill_batch.py",       # Batch layer (large gaps)
    "week6_merge.py",                # Serving layer
]

print("\n[1] TEMPORARY/DEBUG FILES (CÓ THỂ XÓA NGAY):")
for f in temp_files:
    print(f"   ❌ {f}")

print(f"\n   Total: {len(temp_files)} files")

print("\n[2] OLD BACKUP FOLDERS (CÓ THỂ XÓA SAU KHI VERIFY):")
for f in old_folders:
    print(f"   ⚠️  {f}")
print(f"\n   Total: {len(old_folders)} folders")

print("\n[3] TEMPORARY SCRIPTS (CÓ THỂ XÓA):")
for f in temp_scripts:
    print(f"   ❌ {f}")
print(f"\n   Total: {len(temp_scripts)} files")

print("\n[4] UNUSED DATA FOLDERS (CÓ THỂ XÓA NẾU KHÔNG CẦN ANALYSIS):")
for f in unused_folders:
    print(f"   ⚠️  {f}")
print(f"\n   Total: {len(unused_folders)} folders")

print("\n[5] CORE FILES (✅ GIỮ LẠI):")
for f in keep_files:
    print(f"   ✅ {f}")
print(f"\n   Total: {len(keep_files)} files")

print("\n" + "="*80)
print("CLEANUP COMMANDS (PowerShell)")
print("="*80)

print("\n# Xóa debug scripts:")
print("Remove-Item check_data.py, check_forecast.py, final_verification.py")

print("\n# Xóa temp script:")
print("Remove-Item week6_merge_temp.py")

print("\n# Xóa old backups (sau khi verify):")
print("Remove-Item -Recurse -Force data_analysis/week4_*_old")

print("\n# Xóa analysis folders (nếu không cần):")
print("Remove-Item -Recurse -Force data_analysis/daily_raw_csv,data_analysis/large_gaps_top100,data_analysis/missing_days,data_analysis/per_day_counts,data_analysis/results,data_analysis/visualizations")

print("\n" + "="*80)
print("ESTIMATED SPACE SAVINGS")
print("="*80)

import os

total_temp = 0
total_old = 0
total_unused = 0

# Calculate temp files
for f in temp_files:
    if os.path.exists(f):
        total_temp += os.path.getsize(f)

# Calculate old folders
for folder in old_folders:
    if os.path.exists(folder):
        for root, dirs, files in os.walk(folder):
            for file in files:
                total_old += os.path.getsize(os.path.join(root, file))

# Calculate unused folders
for folder in unused_folders:
    if os.path.exists(folder):
        for root, dirs, files in os.walk(folder):
            for file in files:
                total_unused += os.path.getsize(os.path.join(root, file))

# week6_merge_temp.py
if os.path.exists("week6_merge_temp.py"):
    total_temp += os.path.getsize("week6_merge_temp.py")

print(f"\nDebug scripts:     {total_temp/1024:.1f} KB")
print(f"Old backups:       {total_old/1024:.1f} KB")
print(f"Unused folders:    {total_unused/1024/1024:.1f} MB")
print(f"Total savings:     {(total_temp + total_old + total_unused)/1024/1024:.1f} MB")
