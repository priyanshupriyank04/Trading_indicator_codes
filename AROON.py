import pandas as pd
from main import get_results_store  # Import function instead of variable


def calculate_aroon(df, column_high="high", column_low="low", window=14):
    """
    Calculate the Aroon Up and Aroon Down indicators for each ticker in a multi-index DataFrame.

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.
        column_high (str): Column name to use for highest highs.
        column_low (str): Column name to use for lowest lows.
        window (int): Lookback period for Aroon calculation.

    Returns:
        pd.DataFrame: DataFrame with Aroon Up and Aroon Down columns added.
    """
    tickers = df.columns.levels[0]  # Extract tickers from level 0 of the MultiIndex columns

    for ticker in tickers:
        if column_high in df[ticker].columns and column_low in df[ticker].columns:
            high_series = df[(ticker, column_high)]
            low_series = df[(ticker, column_low)]

            def aroon_up(series):
                return series.rolling(window + 1).apply(
                    lambda x: 100 * (window - x[::-1].argmax()) / window, raw=True
                )

            def aroon_down(series):
                return series.rolling(window + 1).apply(
                    lambda x: 100 * (window - x[::-1].argmin()) / window, raw=True
                )

            aroon_up_series = aroon_up(high_series)
            aroon_down_series = aroon_down(low_series)

            df[(ticker, f"Aroon_Up_{window}")] = aroon_up_series
            df[(ticker, f"Aroon_Down_{window}")] = aroon_down_series
        else:
            print(f"Warning: '{column_high}' or '{column_low}' column not found for {ticker}.")

    return df


def run(identifier, df_name, column_high="high", column_low="low", window=14):
    """
    Retrieve stored data and compute the Aroon indicator.

    Args:
        identifier (str): Node ID (used for logging or metadata).
        df_name (str): Name key to fetch the correct DataFrame from results_store.
        column_high (str): Column to use for highest highs.
        column_low (str): Column to use for lowest lows.
        window (int): Lookback period for Aroon calculation.

    Returns:
        pd.DataFrame or None: Updated DataFrame with Aroon columns, if data exists.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty. Check CSV data and columns.")
        return None

    return calculate_aroon(df, column_high, column_low, window)
