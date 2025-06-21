import pandas as pd
import numpy as np
from main import get_results_store  # Import function instead of variable


def calculate_wma(df, column, window):
    """
    Calculate the Weighted Moving Average (WMA) for a given column in a multi-index DataFrame.

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.
        column (str): The column name for which WMA is to be calculated.
        window (int): The WMA window size.

    Returns:
        pd.DataFrame: DataFrame with the WMA column added for each ticker.
    """
    tickers = df.columns.levels[0]  # Extract tickers from level 0 of the MultiIndex columns
    weights = np.arange(1, window + 1)  # Linear weights: 1, 2, ..., window

    for ticker in tickers:
        if column in df[ticker].columns:
            wma = df[ticker][column].rolling(window).apply(
                lambda x: np.dot(x, weights) / weights.sum(), raw=True
            )
            df[(ticker, f"{column}_WMA")] = wma
        else:
            print(f"Warning: '{column}' column not found for {ticker}.")

    return df


def run(identifier, df_name, column="close", window=14):
    """
    Retrieve stored data and compute the WMA for the specified column.

    Args:
        identifier (str): Node ID (used for logging or metadata).
        df_name (str): Name key to fetch the correct DataFrame from results_store.
        column (str): Column to compute WMA on.
        window (int): WMA window size.

    Returns:
        pd.DataFrame or None: Updated DataFrame with WMA column, if data exists.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty. Check CSV data and columns.")
        return None

    return calculate_wma(df, column, window)
