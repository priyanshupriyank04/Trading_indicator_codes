import pandas as pd
from main import get_results_store  # Import function instead of variable


def calculate_acd(df, column_high="high", column_low="low", column_close="close", column_volume="volume"):
    """
    Calculate the Accumulation/Distribution Line (A/D Line) for a given OHLCV multi-index DataFrame.

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.
        column_high (str): Column name for high prices.
        column_low (str): Column name for low prices.
        column_close (str): Column name for close prices.
        column_volume (str): Column name for volume.

    Returns:
        pd.DataFrame: DataFrame with A/D Line column added for each ticker.
    """
    tickers = df.columns.levels[0]  # Extract tickers from level 0 of the MultiIndex columns

    for ticker in tickers:
        if all(col in df[ticker].columns for col in [column_high, column_low, column_close, column_volume]):
            high = df[(ticker, column_high)]
            low = df[(ticker, column_low)]
            close = df[(ticker, column_close)]
            volume = df[(ticker, column_volume)]

            # Money Flow Multiplier
            mfm = ((2 * close - low - high) / (high - low)).replace([float('inf'), -float('inf')], 0).fillna(0)
            # Money Flow Volume
            mfv = mfm * volume
            # Accumulation/Distribution Line
            ad_line = mfv.cumsum()

            df[(ticker, "ACD")] = ad_line
        else:
            print(f"Warning: Missing OHLCV columns for ticker {ticker}.")

    return df


def run(identifier, df_name, column_high="high", column_low="low", column_close="close", column_volume="volume"):
    """
    Retrieve stored data and compute the Accumulation/Distribution Line (A/D Line).

    Args:
        identifier (str): Node ID (used for logging or metadata).
        df_name (str): Name key to fetch the correct DataFrame from results_store.
        column_high (str): Column name for high prices.
        column_low (str): Column name for low prices.
        column_close (str): Column name for close prices.
        column_volume (str): Column name for volume.

    Returns:
        pd.DataFrame or None: Updated DataFrame with A/D Line column, if data exists.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty. Check CSV data and columns.")
        return None

    return calculate_acd(df, column_high, column_low, column_close, column_volume)
