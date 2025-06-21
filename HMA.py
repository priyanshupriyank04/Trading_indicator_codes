import pandas as pd
import numpy as np
from main import get_results_store


def wma(series, period):
    """
    Weighted Moving Average
    """
    weights = np.arange(1, period + 1)
    return series.rolling(period).apply(lambda x: np.dot(x, weights) / weights.sum(), raw=True)


def calculate_hma(df, column, window):
    """
    Calculate the Hull Moving Average (HMA) for a given column in a multi-index DataFrame.

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.
        column (str): The column name for which HMA is to be calculated.
        window (int): The HMA window size.

    Returns:
        pd.DataFrame: DataFrame with the HMA column added for each ticker.
    """
    tickers = df.columns.levels[0]

    for ticker in tickers:
        if column not in df[ticker].columns:
            print(f"Warning: '{column}' column not found for {ticker}.")
            continue

        series = df[(ticker, column)]

        half_length = int(window / 2)
        sqrt_length = int(np.sqrt(window))

        wma_half = wma(series, half_length)
        wma_full = wma(series, window)
        hma = wma(2 * wma_half - wma_full, sqrt_length)

        df[(ticker, f"{column}_HMA")] = hma

    return df


def run(identifier, df_name, column="close", window=9):
    """
    Retrieve stored data and compute the HMA for the specified column.

    Args:
        identifier (str): Node ID (used for logging or metadata).
        df_name (str): Name key to fetch the correct DataFrame from results_store.
        column (str): Column to compute HMA on.
        window (int): HMA window size (default: 9).

    Returns:
        pd.DataFrame or None: Updated DataFrame with HMA column, if data exists.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty. Check CSV data and columns.")
        return None

    return calculate_hma(df, column, window)
