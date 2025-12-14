import pandas as pd

# Check BTC CSV
btc = pd.read_csv('data/btc/BTCUSDT_1min_2012-2025.csv')
btc['date'] = pd.to_datetime(btc['Timestamp']).dt.date
print(f'BTC CSV: {len(btc):,} 1-min rows')
print(f'BTC unique dates: {btc["date"].nunique():,} days')
print(f'BTC date range: {btc["date"].min()} → {btc["date"].max()}')

# Check ETH CSV
eth = pd.read_csv('data/eth/ETHUSDT_1min_2017-2025.csv')
eth['date'] = pd.to_datetime(eth['Timestamp']).dt.date
print(f'\nETH CSV: {len(eth):,} 1-min rows')
print(f'ETH unique dates: {eth["date"].nunique():,} days')
print(f'ETH date range: {eth["date"].min()} → {eth["date"].max()}')
