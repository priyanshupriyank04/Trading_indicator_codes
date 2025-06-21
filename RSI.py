import pandas as pd
from main import get_results_store  # Import function to access stored data


def rma(series, window):
    """
    Wilder's RMA (Recursive Moving Average), equivalent to Pine Script's ta.rma().
    Uses exponential weighting with alpha = 1 / window.
    """
    alpha = 1 / window
    return series.ewm(alpha=alpha, adjust=False).mean()


def calculate_rsi(df, column, window):
    """
    Calculate the Relative Strength Index (RSI) using RMA smoothing.

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.
        column (str): Column name to calculate RSI on (typically 'close').
        window (int): RSI lookback window (default 14).

    Returns:
        pd.DataFrame: Updated DataFrame with RSI column for each ticker.
    """
    tickers = df.columns.levels[0]  # Extract ticker symbols

    for ticker in tickers:
        if column not in df[ticker].columns:
            print(f"Warning: '{column}' column not found for {ticker}.")
            continue

        close_series = df[(ticker, column)]
        delta = close_series.diff()

        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)

        avg_gain = rma(gain, window)
        avg_loss = rma(loss, window)

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        df[(ticker, f"{column}_RSI")] = rsi

    return df


def run(identifier, df_name, column="close", window=14):
    """
    Fetches data and computes RSI using RMA smoothing.

    Args:
        identifier (str): Node ID for logging or metadata.
        df_name (str): Name of the DataFrame in results_store.
        column (str): Column to compute RSI on (default: 'close').
        window (int): Lookback period (default: 14).

    Returns:
        pd.DataFrame or None: DataFrame with RSI column, or None if errors occur.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty. Check CSV input.")
        return None

    return calculate_rsi(df, column, window)
