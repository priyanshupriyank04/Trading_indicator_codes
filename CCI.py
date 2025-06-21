import pandas as pd
from main import get_results_store


def calculate_cci(df, column_high='high', column_low='low', column_close='close',
                  window=20, ma_type='SMA'):
    """
    Calculate the Commodity Channel Index (CCI) for each ticker in a multi-index DataFrame.

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.
        column_high (str): Column for high prices.
        column_low (str): Column for low prices.
        column_close (str): Column for close prices.
        window (int): Lookback period for CCI calculation.
        ma_type (str): Type of moving average ('SMA', 'EMA', 'RMA', 'WMA', 'VWMA').

    Returns:
        pd.DataFrame: DataFrame with CCI column added for each ticker.
    """
    tickers = df.columns.levels[0]

    for ticker in tickers:
        if not all(col in df[ticker].columns for col in [column_high, column_low, column_close]):
            print(f"Missing OHLC columns for {ticker}, skipping...")
            continue

        high = df[(ticker, column_high)]
        low = df[(ticker, column_low)]
        close = df[(ticker, column_close)]

        typical_price = (high + low + close) / 3

        ma_type = ma_type.upper()
        if ma_type == 'SMA':
            tp_ma = typical_price.rolling(window=window).mean()
        elif ma_type == 'EMA':
            tp_ma = typical_price.ewm(span=window, adjust=False).mean()
        elif ma_type in ['RMA', 'SMMA']:
            tp_ma = typical_price.ewm(alpha=1 / window, adjust=False).mean()
        elif ma_type == 'WMA':
            weights = pd.Series(range(1, window + 1))
            tp_ma = typical_price.rolling(window).apply(lambda x: (x * weights).sum() / weights.sum(), raw=True)
        elif ma_type == 'VWMA':
            try:
                volume = df[(ticker, 'volume')]
                tp_ma = (
                    (typical_price * volume)
                    .rolling(window=window)
                    .sum() /
                    volume.rolling(window=window).sum()
                )
            except KeyError:
                print(f"Missing 'volume' for VWMA in {ticker}, skipping...")
                continue
        else:
            print(f"Unknown MA type '{ma_type}' for {ticker}, skipping...")
            continue

        mean_dev = typical_price.rolling(window=window).apply(
            lambda x: (abs(x - x.mean())).mean(), raw=True
        )

        cci = (typical_price - tp_ma) / (0.015 * mean_dev)
        df[(ticker, f"CCI_{window}_{ma_type}")] = cci

    return df


def run(identifier, df_name, column_high='high', column_low='low', column_close='close',
        window=20, ma_type='SMA'):
    """
    Run CCI calculation.

    Args:
        identifier (str): Node ID for logging or metadata.
        df_name (str): Key to retrieve DataFrame from results_store.
        column_high (str): Column for high prices.
        column_low (str): Column for low prices.
        column_close (str): Column for close prices.
        window (int): Lookback period.
        ma_type (str): Type of moving average.

    Returns:
        pd.DataFrame or None: Updated DataFrame with CCI column, or None on failure.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty.")
        return None

    return calculate_cci(df, column_high, column_low, column_close, window, ma_type)
