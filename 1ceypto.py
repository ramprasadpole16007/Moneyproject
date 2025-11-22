from binance.client import Client
import pandas as pd
import datetime as dt
import requests

# Create Binance client
client = Client()

# -----------------------------------------------------
# Fetch circulating supply from CoinGecko
# -----------------------------------------------------
def get_circulating_supply(coin_id="bitcoin"):
    """Returns circulating supply of a coin using CoinGecko API."""
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
    response = requests.get(url).json()
    return response["market_data"]["circulating_supply"]


# -----------------------------------------------------
# Download OHLCV data from Binance
# -----------------------------------------------------
def get_binance_data(symbol, interval, years):
    """Downloads historical OHLCV data for given symbol and interval."""
    
    # Convert years → milliseconds range
    ms_per_day = 24 * 60 * 60 * 1000
    end_time = int(dt.datetime.now().timestamp() * 1000)
    start_time = end_time - (years * 365 * ms_per_day)

    # Fetch raw kline data
    raw_data = client.get_historical_klines(symbol, interval, start_time, end_time)

    # Binance OHLCV format
    columns = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base", "taker_buy_quote", "ignore"
    ]

    df = pd.DataFrame(raw_data, columns=columns)

    # Convert timestamps
    df["open_time"]  = pd.to_datetime(df["open_time"],  unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")

    # Convert numeric columns safely
    numeric_columns = ["open", "high", "low", "close", "volume"]
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Sort data by open_time
    df.sort_values("open_time", inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df


# -----------------------------------------------------
# Clean dataset (fix duplicates & missing values)
# -----------------------------------------------------
def clean_dataframe(df):
    df = df.drop_duplicates(subset=["open_time"])
    df = df.fillna(method="ffill").fillna(method="bfill")
    return df


# -----------------------------------------------------
# Add market cap column
# -----------------------------------------------------
def add_market_cap(df, coin_id="bitcoin"):
    """Adds a market_cap column = close_price × circulating_supply."""
    supply = get_circulating_supply(coin_id)
    df["market_cap"] = df["close"] * supply
    return df


# -----------------------------------------------------
# BTC DAILY
# -----------------------------------------------------
btc_daily = get_binance_data("BTCUSDT", Client.KLINE_INTERVAL_1DAY, 5)
btc_daily = clean_dataframe(btc_daily)
btc_daily = add_market_cap(btc_daily, "bitcoin")
btc_daily.to_csv("btc_daily_with_marketcap.csv", index=False)

# -----------------------------------------------------
# BTC WEEKLY
# -----------------------------------------------------
btc_weekly = get_binance_data("BTCUSDT", Client.KLINE_INTERVAL_1WEEK, 5)
btc_weekly = clean_dataframe(btc_weekly)
btc_weekly = add_market_cap(btc_weekly, "bitcoin")
btc_weekly.to_csv("btc_weekly_with_marketcap.csv", index=False)

# -----------------------------------------------------
# BTC HOURLY
# -----------------------------------------------------
btc_hourly = get_binance_data("BTCUSDT", Client.KLINE_INTERVAL_1HOUR, 5)
btc_hourly = clean_dataframe(btc_hourly)
btc_hourly = add_market_cap(btc_hourly, "bitcoin")
btc_hourly.to_csv("btc_hourly_with_marketcap.csv", index=False)

print("Saved all BTC files:")
print(" - btc_daily_with_marketcap.csv")
print(" - btc_weekly_with_marketcap.csv")
print(" - btc_hourly_with_marketcap.csv")
