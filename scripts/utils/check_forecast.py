import pandas as pd
import os

print("="*80)
print("FORECAST RESULTS FOR BOTH COINS")
print("="*80)

for symbol in ['BTCUSDT', 'ETHUSDT']:
    csv = f'data_analysis/week4_results/{symbol}_actual_vs_pred.csv'
    
    if os.path.exists(csv):
        df = pd.read_csv(csv)
        print(f"\n{symbol}:")
        print(f"  Total predictions: {len(df):,} days")
        print(f"  Date range: {df['ds'].min()} → {df['ds'].max()}")
        
        # Last 5 predictions
        print(f"\n  Last 5 days (actual vs predicted):")
        last5 = df[['ds', 'y', 'yhat']].tail(5)
        last5['error'] = ((last5['yhat'] - last5['y']) / last5['y'] * 100).round(2)
        print(last5.to_string(index=False))
        
        # Visualization
        viz = f'data_analysis/week4_visualizations/{symbol}_forecast_interactive.html'
        if os.path.exists(viz):
            print(f"\n  ✅ Interactive chart: {viz}")
        
        # Future forecast
        parquet = f'data_analysis/week4_forecasts/{symbol}_forecast.parquet'
        if os.path.exists(parquet):
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.appName("Check").getOrCreate()
            df_forecast = spark.read.parquet(parquet).toPandas()
            
            # Get future predictions (after last actual data)
            last_actual_date = df['ds'].max()
            future = df_forecast[df_forecast['ds'] > last_actual_date]
            
            if len(future) > 0:
                print(f"\n  🔮 Future forecast: {len(future)} days ahead")
                print(f"  Future range: {future['ds'].min()} → {future['ds'].max()}")
                print(f"\n  Next 7 days prediction:")
                next7 = future[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].head(7)
                print(next7.to_string(index=False))
            
            spark.stop()
    
    print("\n" + "-"*80)
