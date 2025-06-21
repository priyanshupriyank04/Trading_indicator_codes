import pandas as pd
from main import get_results_store


def calculate_obv(df, column_close='close', column_volume='volume'):
    """
    Calculate the On Balance Volume (OBV) for each ticker in a multi-index DataFrame.

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.
        column_close (str): Column name for closing prices.
        column_volume (str): Column name for volume.

    Returns:
        pd.DataFrame: Updated DataFrame with OBV column for each ticker.
    """
    tickers = df.columns.levels[0]

    for ticker in tickers:
        columns = df[ticker].columns
        if not all(col in columns for col in [column_close, column_volume]):
            print(f"Warning: Required columns '{column_close}' or '{column_volume}' not found for {ticker}. Skipping...")
            continue

        close = df[(ticker, column_close)]
        volume = df[(ticker, column_volume)]

        direction = close.diff().apply(lambda x: 1 if x > 0 else -1 if x < 0 else 0)
        obv = (volume * direction).fillna(0).cumsum()

        df[(ticker, 'OBV')] = obv

    return df


def run(identifier, df_name, column_close='close', column_volume='volume'):
    """
    Retrieve stored data and compute the OBV for each ticker.

    Args:
        identifier (str): Node ID for tracking/logging.
        df_name (str): Key to fetch the correct DataFrame.
        column_close (str): Column to use for close prices.
        column_volume (str): Column to use for volume.

    Returns:
        pd.DataFrame or None: Updated DataFrame with OBV column.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty.")
        return None

    return calculate_obv(df, column_close, column_volume)
