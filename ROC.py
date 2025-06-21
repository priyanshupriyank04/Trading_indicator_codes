import pandas as pd
from main import get_results_store  # Import function instead of variable


def calculate_roc(df, column, window):
    """
    Calculate the Rate of Change (ROC) for a given column in a multi-index DataFrame.

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.
        column (str): The column name for which ROC is to be calculated.
        window (int): The lookback period for ROC.

    Returns:
        pd.DataFrame: DataFrame with the ROC column added for each ticker.
    """
    tickers = df.columns.levels[0]  # Extract tickers from level 0 of the MultiIndex columns

    for ticker in tickers:
        if column in df[ticker].columns:
            current = df[(ticker, column)]
            shifted = current.shift(window)
            roc = ((current - shifted) / shifted) * 100
            df[(ticker, f"{column}_ROC_{window}")] = roc
        else:
            print(f"Warning: '{column}' column not found for {ticker}.")

    return df


def run(identifier, df_name, column="close", window=9):
    """
    Retrieve stored data and compute the ROC for the specified column.

    Args:
        identifier (str): Node ID (used for logging or metadata).
        df_name (str): Name key to fetch the correct DataFrame from results_store.
        column (str): Column to compute ROC on.
        window (int): ROC lookback period.

    Returns:
        pd.DataFrame or None: Updated DataFrame with ROC column, if data exists.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty. Check CSV data and columns.")
        return None

    return calculate_roc(df, column, window)
