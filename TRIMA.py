import pandas as pd
from main import get_results_store


def calculate_trima(df, column, window):
    """
    Calculate the Triangular Moving Average (TRIMA) for a given column
    in a multi-index DataFrame using SMA(SMA(x, n), n).

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.
        column (str): The column name to compute TRIMA on (e.g., 'close').
        window (int): The window size for the TRIMA calculation.

    Returns:
        pd.DataFrame: Updated DataFrame with TRIMA column added per ticker.
    """
    tickers = df.columns.levels[0]

    for ticker in tickers:
        if column not in df[ticker].columns:
            print(f"Warning: '{column}' column not found for {ticker}, skipping...")
            continue

        series = df[(ticker, column)]
        sma1 = series.rolling(window=window).mean()
        trima = sma1.rolling(window=window).mean()

        df[(ticker, f"{column}_TRIMA")] = trima

    return df


def run(identifier, df_name, column="close", window=10):
    """
    Retrieve stored data and compute TRIMA for the specified column.

    Args:
        identifier (str): Node ID for logging or metadata.
        df_name (str): Name key to fetch the correct DataFrame.
        column (str): Column to compute TRIMA on (default: 'close').
        window (int): TRIMA window size (default: 10).

    Returns:
        pd.DataFrame or None: Updated DataFrame with TRIMA column, if data exists.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty.")
        return None

    return calculate_trima(df, column, window)
