import pandas as pd
from main import get_results_store  # Import function instead of variable


def calculate_vwma(df, column, window):
    """
    Calculate the Volume-Weighted Moving Average (VWMA) for a given column in a multi-index DataFrame.

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.
        column (str): The price column name for which VWMA is to be calculated (e.g., 'close').
        window (int): The rolling window size for VWMA calculation.

    Returns:
        pd.DataFrame: DataFrame with the VWMA column added for each ticker.
    """
    tickers = df.columns.levels[0]  # Extract tickers from level 0 of the MultiIndex columns

    for ticker in tickers:
        if column in df[ticker].columns and "volume" in df[ticker].columns:
            price = df[ticker][column]
            volume = df[ticker]["volume"]
            vwma = (price * volume).rolling(window=window).sum() / volume.rolling(window=window).sum()
            df[(ticker, f"{column}_VWMA")] = vwma
        else:
            print(f"Warning: Required columns ('{column}' or 'volume') not found for {ticker}.")

    return df


def run(identifier, df_name, column="close", window=14):
    """
    Retrieve stored data and compute the VWMA for the specified column.

    Args:
        identifier (str): Node ID (for logging/debugging purposes).
        df_name (str): Key used to fetch the correct DataFrame from results_store.
        column (str, optional): Price column name to compute VWMA on. Defaults to "close".
        window (int, optional): Window size for VWMA calculation. Defaults to 14.

    Returns:
        pd.DataFrame or None: DataFrame with VWMA added if data exists, else None.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty. Check CSV data and columns.")
        return None

    return calculate_vwma(df, column, window)
