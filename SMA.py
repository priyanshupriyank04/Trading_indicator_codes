import pandas as pd
from main import get_results_store  # Import function instead of variable


def calculate_sma(df, column, window):
    """
    Calculate the Simple Moving Average (SMA) for a given column in a multi-index DataFrame.

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.
        column (str): The column name for which SMA is to be calculated.
        window (int): The rolling window size for SMA calculation.

    Returns:
        pd.DataFrame: DataFrame with the SMA column added for each ticker.
    """
    tickers = df.columns.levels[0]  # Extract tickers from level 0 of the MultiIndex columns

    for ticker in tickers:
        if column in df[ticker].columns:
            df[(ticker, f"{column}_SMA")] = df[(ticker, column)].rolling(window=window).mean()
        else:
            print(f"Warning: '{column}' column not found for {ticker}.")

    return df


def run(identifier, df_name, column="close", window=14):
    """
    Retrieve stored data and compute the SMA for the specified column.

    Args:
        identifier (str): Node ID (for logging/debugging purposes).
        df_name (str): Key used to fetch the correct DataFrame from results_store.
        column (str, optional): Column name to compute SMA on. Defaults to "close".
        window (int, optional): Window size for SMA calculation. Defaults to 14.

    Returns:
        pd.DataFrame or None: DataFrame with SMA added if data exists, else None.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty. Check CSV data and columns.")
        return None

    return calculate_sma(df, column, window)
