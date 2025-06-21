import pandas as pd
from main import get_results_store  # Import function instead of variable

def calculate_true_range(df):
    """
    Calculate the True Range (TR) for each ticker.

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.

    Returns:
        pd.DataFrame: DataFrame with the TR column added for each ticker.
    """
    tickers = df.columns.levels[0]  # Extract tickers from level 0

    for ticker in tickers:
        try:
            high = df[(ticker, 'high')]
            low = df[(ticker, 'low')]
            prev_close = df[(ticker, 'close')].shift(1)
        except KeyError:
            print(f"Missing columns for {ticker}, skipping...")
            continue

        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()

        true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df[(ticker, 'TR')] = true_range

    return df

def run(identifier, df_name):
    """
    Compute the True Range (TR) for a given dataframe name.

    Args:
        identifier (str): Node identifier (for logging).
        df_name (str): DataFrame name key in results_store.

    Returns:
        pd.DataFrame or None: Updated DataFrame with TR column.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty.")
        return None

    return calculate_true_range(df)
