import pandas as pd
from main import get_results_store


def calculate_cmf(df, column_high="high", column_low="low", column_close="close", column_volume="volume", window=20):
    """
    Calculate the Chaikin Money Flow (CMF) indicator for each ticker in a multi-index DataFrame.

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.
        column_high (str): Column name for high prices.
        column_low (str): Column name for low prices.
        column_close (str): Column name for close prices.
        column_volume (str): Column name for volume.
        window (int): Lookback period for CMF calculation.

    Returns:
        pd.DataFrame: DataFrame with CMF column added for each ticker.
    """
    tickers = df.columns.levels[0]

    for ticker in tickers:
        if all(col in df[ticker].columns for col in [column_high, column_low, column_close, column_volume]):
            high = df[(ticker, column_high)]
            low = df[(ticker, column_low)]
            close = df[(ticker, column_close)]
            volume = df[(ticker, column_volume)]

            # Avoid division by zero
            price_range = (high - low).replace(0, pd.NA)

            # Money Flow Multiplier
            mfm = ((2 * close - high - low) / price_range).fillna(0)

            # Money Flow Volume
            mfv = mfm * volume

            # Chaikin Money Flow
            cmf = mfv.rolling(window=window).sum() / volume.rolling(window=window).sum()

            df[(ticker, f'CMF_{window}')] = cmf
        else:
            print(f"Warning: Missing OHLCV columns for {ticker}, skipping...")

    return df


def run(identifier, df_name, column_high="high", column_low="low", column_close="close",
        column_volume="volume", window=20):
    """
    Retrieve stored data and compute Chaikin Money Flow (CMF).

    Args:
        identifier (str): Node ID (used for logging or metadata).
        df_name (str): Name key to fetch the correct DataFrame from results_store.
        column_high (str): Column for high prices.
        column_low (str): Column for low prices.
        column_close (str): Column for close prices.
        column_volume (str): Column for volume.
        window (int): Lookback period for CMF.

    Returns:
        pd.DataFrame or None: Updated DataFrame with CMF column, if data exists.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty.")
        return None

    return calculate_cmf(df, column_high, column_low, column_close, column_volume, window)
