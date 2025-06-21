import pandas as pd
from main import get_results_store  # Import function to access stored data


def calculate_dema(df, column, window):
    """
    Calculate the Double Exponential Moving Average (DEMA) for a given column
    in a multi-index DataFrame.

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.
        column (str): The column name for which DEMA is to be calculated.
        window (int): The DEMA window size.

    Returns:
        pd.DataFrame: DataFrame with the DEMA column added for each ticker.
    """
    tickers = df.columns.levels[0]

    for ticker in tickers:
        if column not in df[ticker].columns:
            print(f"Warning: '{column}' column not found for {ticker}.")
            continue

        series = df[(ticker, column)]
        ema1 = series.ewm(span=window, adjust=False).mean()
        ema2 = ema1.ewm(span=window, adjust=False).mean()

        dema = 2 * ema1 - ema2

        df[(ticker, f"{column}_DEMA")] = dema

    return df


def run(identifier, df_name, column="close", window=9):
    """
    Retrieve stored data and compute the DEMA for the specified column.

    Args:
        identifier (str): Node ID for logging or metadata.
        df_name (str): Name key to fetch the correct DataFrame.
        column (str): Column to compute DEMA on (default: 'close').
        window (int): DEMA window size (default: 9).

    Returns:
        pd.DataFrame or None: Updated DataFrame with DEMA column, if data exists.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty. Check CSV data and columns.")
        return None

    return calculate_dema(df, column, window)
