import pandas as pd
from main import get_results_store  # Import function to access stored data


def calculate_stochastic(df, column_close, column_high, column_low, k_period, k_smooth, d_period):
    """
    Calculate the Stochastic Oscillator (%K and %D) for a multi-index DataFrame.

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.
        column_close (str): Column name for closing prices.
        column_high (str): Column name for high prices.
        column_low (str): Column name for low prices.
        k_period (int): Lookback period for %K.
        k_smooth (int): Smoothing for %K line.
        d_period (int): Smoothing period for %D line.

    Returns:
        pd.DataFrame: DataFrame with %K and %D columns added.
    """
    tickers = df.columns.levels[0]

    for ticker in tickers:
        if not all(col in df[ticker].columns for col in [column_close, column_high, column_low]):
            print(f"Warning: One or more required columns ('{column_close}', '{column_high}', '{column_low}') not found for {ticker}.")
            continue

        close = df[(ticker, column_close)]
        high = df[(ticker, column_high)].rolling(window=k_period).max()
        low = df[(ticker, column_low)].rolling(window=k_period).min()

        raw_k = ((close - low) / (high - low)) * 100
        smooth_k = raw_k.rolling(window=k_smooth).mean()
        d_line = smooth_k.rolling(window=d_period).mean()

        df[(ticker, f"%K_{k_period}_{k_smooth}")] = smooth_k
        df[(ticker, f"%D_{k_period}_{k_smooth}_{d_period}")] = d_line

    return df


def run(identifier, df_name, column_close="close", column_high="high", column_low="low",
        k_period=14, k_smooth=1, d_period=3):
    """
    Retrieve stored data and compute the Stochastic Oscillator for the specified columns.

    Args:
        identifier (str): Node ID for logging or metadata.
        df_name (str): Name key to fetch the correct DataFrame.
        column_close (str): Column for close prices.
        column_high (str): Column for high prices.
        column_low (str): Column for low prices.
        k_period (int): Lookback period for %K.
        k_smooth (int): Smoothing for %K.
        d_period (int): Smoothing for %D.

    Returns:
        pd.DataFrame or None: Updated DataFrame with Stochastic Oscillator columns, if data exists.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty. Check CSV data and columns.")
        return None

    return calculate_stochastic(df, column_close, column_high, column_low, k_period, k_smooth, d_period)
