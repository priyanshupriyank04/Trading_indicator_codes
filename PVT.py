import pandas as pd
from main import get_results_store


def calculate_pvt(df, column_close="close", column_volume="volume"):
    """
    Calculate the Price Volume Trend (PVT) for each ticker in a multi-index DataFrame.

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.
        column_close (str): Column name for closing prices.
        column_volume (str): Column name for volume.

    Returns:
        pd.DataFrame: DataFrame with PVT column added for each ticker.
    """
    tickers = df.columns.levels[0]

    for ticker in tickers:
        if all(col in df[ticker].columns for col in [column_close, column_volume]):
            close = df[(ticker, column_close)]
            volume = df[(ticker, column_volume)]

            pct_change = close.pct_change().fillna(0)
            pvt = (pct_change * volume).cumsum()

            df[(ticker, "PVT")] = pvt
        else:
            print(f"Warning: Missing close or volume column for {ticker}, skipping...")

    return df


def run(identifier, df_name, column_close="close", column_volume="volume"):
    """
    Retrieve stored data and compute Price Volume Trend (PVT).

    Args:
        identifier (str): Node ID (used for logging or metadata).
        df_name (str): Name key to fetch the correct DataFrame from results_store.
        column_close (str): Column name for closing prices.
        column_volume (str): Column name for volume.

    Returns:
        pd.DataFrame or None: Updated DataFrame with PVT column, if data exists.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty.")
        return None

    return calculate_pvt(df, column_close, column_volume)
