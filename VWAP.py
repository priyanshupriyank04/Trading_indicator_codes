import pandas as pd
from main import get_results_store  # Import function instead of variable


def calculate_vwap(df):
    """
    Calculate the Volume Weighted Average Price (VWAP) for each ticker in a multi-index DataFrame.
    VWAP = cumulative(sum(price * volume)) / cumulative(sum(volume))

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0. Must contain 'high', 'low', 'close', 'volume'.

    Returns:
        pd.DataFrame: DataFrame with 'VWAP' column added per ticker.
    """
    tickers = df.columns.levels[0]

    for ticker in tickers:
        try:
            high = df[(ticker, 'high')]
            low = df[(ticker, 'low')]
            close = df[(ticker, 'close')]
            volume = df[(ticker, 'volume')]
        except KeyError:
            print(f"Missing columns for {ticker}, skipping...")
            continue

        typical_price = (high + low + close) / 3
        vwap = (typical_price * volume).cumsum() / volume.cumsum()
        df[(ticker, 'VWAP')] = vwap

    return df


def run(identifier, df_name):
    """
    Retrieve stored data and compute the VWAP for each ticker.

    Args:
        identifier (str): Node ID (for logging/debugging purposes).
        df_name (str): Key used to fetch the correct DataFrame from results_store.

    Returns:
        pd.DataFrame or None: DataFrame with VWAP added if data exists, else None.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty. Check CSV data and columns.")
        return None

    return calculate_vwap(df)
