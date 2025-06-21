import pandas as pd
from main import get_results_store


def calculate_momentum(df, column, window):
    """
    Calculate the Momentum indicator for each ticker in a multi-index DataFrame.

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.
        column (str): Column name to compute momentum on (e.g., 'close').
        window (int): Lookback period for momentum calculation.

    Returns:
        pd.DataFrame: Updated DataFrame with Momentum column for each ticker.
    """
    tickers = df.columns.levels[0]

    for ticker in tickers:
        if column not in df[ticker].columns:
            print(f"Warning: Column '{column}' not found for {ticker}.")
            continue

        price = df[(ticker, column)]
        momentum = price - price.shift(window)
        df[(ticker, f"{column}_MOM_{window}")] = momentum

    return df


def run(identifier, df_name, column="close", window=10):
    """
    Retrieve stored data and compute the Momentum indicator for each ticker.

    Args:
        identifier (str): Node ID for tracking/logging.
        df_name (str): Key to fetch the correct DataFrame.
        column (str): Column name to compute momentum on.
        window (int): Lookback period for momentum.

    Returns:
        pd.DataFrame or None: Updated DataFrame with Momentum column.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty.")
        return None

    return calculate_momentum(df, column, window)
