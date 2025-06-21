import pandas as pd
from main import get_results_store  # Import function instead of variable


def calculate_williams_r(df, column_close, column_high, column_low, window):
    """
    Calculate the Williams %R indicator for a multi-index DataFrame.

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.
        column_close (str): Column name for closing prices.
        column_high (str): Column name for high prices.
        column_low (str): Column name for low prices.
        window (int): Lookback period for Williams %R.

    Returns:
        pd.DataFrame: DataFrame with the Williams %R column added for each ticker.
    """
    tickers = df.columns.levels[0]

    for ticker in tickers:
        if not all(col in df[ticker].columns for col in [column_close, column_high, column_low]):
            print(f"Warning: One or more required columns ('{column_close}', '{column_high}', '{column_low}') not found for {ticker}.")
            continue

        close = df[(ticker, column_close)]
        high = df[(ticker, column_high)].rolling(window=window).max()
        low = df[(ticker, column_low)].rolling(window=window).min()

        willr = 100 * (close - high) / (high - low)
        df[(ticker, f"%R_{window}")] = willr

    return df


def run(identifier, df_name, column_close="close", column_high="high", column_low="low", window=14):
    """
    Retrieve stored data and compute the Williams %R for the specified columns.

    Args:
        identifier (str): Node ID (used for logging or metadata).
        df_name (str): Name key to fetch the correct DataFrame from results_store.
        column_close (str): Column to use for close prices.
        column_high (str): Column to use for high prices.
        column_low (str): Column to use for low prices.
        window (int): Lookback window for the calculation (default: 14).

    Returns:
        pd.DataFrame or None: Updated DataFrame with Williams %R column, if data exists.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty. Check CSV data and columns.")
        return None

    return calculate_williams_r(df, column_close, column_high, column_low, window)
