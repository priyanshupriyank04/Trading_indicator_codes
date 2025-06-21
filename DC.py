import pandas as pd
from main import get_results_store  # Import function instead of variable


def calculate_donchian_channel(df, column_high="high", column_low="low", window=20):
    """
    Calculate the Donchian Channel for a given high and low column in a multi-index DataFrame.

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.
        column_high (str): Column name to use for highest highs (typically 'high').
        column_low (str): Column name to use for lowest lows (typically 'low').
        window (int): The rolling window size for Donchian Channel calculation.

    Returns:
        pd.DataFrame: DataFrame with Donchian Channel columns (Upper, Lower, Basis) added.
    """
    tickers = df.columns.levels[0]  # Extract tickers from level 0 of the MultiIndex columns

    for ticker in tickers:
        if column_high in df[ticker].columns and column_low in df[ticker].columns:
            upper = df[(ticker, column_high)].rolling(window=window).max()
            lower = df[(ticker, column_low)].rolling(window=window).min()
            basis = (upper + lower) / 2

            df[(ticker, f"DC_upper_{window}")] = upper
            df[(ticker, f"DC_lower_{window}")] = lower
            df[(ticker, f"DC_basis_{window}")] = basis
        else:
            print(f"Warning: '{column_high}' or '{column_low}' column not found for {ticker}.")

    return df


def run(identifier, df_name, column_high="high", column_low="low", window=20):
    """
    Retrieve stored data and compute the Donchian Channel (upper, lower, basis).

    Args:
        identifier (str): Node ID (used for logging or metadata).
        df_name (str): Name key to fetch the correct DataFrame from results_store.
        column_high (str): Column to use for highest highs (typically 'high').
        column_low (str): Column to use for lowest lows (typically 'low').
        window (int): Lookback period for the Donchian Channel.

    Returns:
        pd.DataFrame or None: Updated DataFrame with Donchian Channel columns, if data exists.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty. Check CSV data and columns.")
        return None

    return calculate_donchian_channel(df, column_high, column_low, window)
