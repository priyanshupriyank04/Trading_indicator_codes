import pandas as pd
from main import get_results_store  # Import function instead of variable


def calculate_efi(df, column_close="close", column_volume="volume", window=13):
    """
    Calculate the Elder Force Index (EFI) for each ticker in a multi-index DataFrame.

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.
        column_close (str): Column name for closing prices.
        column_volume (str): Column name for volume data.
        window (int): EMA smoothing window for EFI calculation.

    Returns:
        pd.DataFrame: DataFrame with EFI column added for each ticker.
    """
    tickers = df.columns.levels[0]  # Extract tickers from level 0 of the MultiIndex columns

    for ticker in tickers:
        if column_close in df[ticker].columns and column_volume in df[ticker].columns:
            close = df[(ticker, column_close)]
            volume = df[(ticker, column_volume)]

            efi_raw = close.diff() * volume
            efi_ema = efi_raw.ewm(span=window, adjust=False).mean()

            df[(ticker, f"EFI_{window}")] = efi_ema
        else:
            print(f"Warning: '{column_close}' or '{column_volume}' column not found for {ticker}.")

    return df


def run(identifier, df_name, column_close="close", column_volume="volume", window=13):
    """
    Retrieve stored data and compute the Elder Force Index (EFI).

    Args:
        identifier (str): Node ID (used for logging or metadata).
        df_name (str): Name key to fetch the correct DataFrame from results_store.
        column_close (str): Column name for closing prices.
        column_volume (str): Column name for volume data.
        window (int): EMA smoothing window for EFI calculation.

    Returns:
        pd.DataFrame or None: Updated DataFrame with EFI column, if data exists.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty. Check CSV data and columns.")
        return None

    return calculate_efi(df, column_close, column_volume, window)
