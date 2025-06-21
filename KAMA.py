import pandas as pd
from main import get_results_store  # Import function to access stored data


def calculate_kama(df, column, window):
    """
    Calculate the Kaufman Adaptive Moving Average (KAMA) for a given column
    in a multi-index DataFrame.

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.
        column (str): The column name to compute KAMA on (typically 'close').
        window (int): Lookback window for KAMA (default: 21).

    Returns:
        pd.DataFrame: DataFrame with the KAMA column added for each ticker.
    """
    tickers = df.columns.levels[0]

    fast_alpha = 2 / (2 + 1)    # Fastest EMA smoothing constant
    slow_alpha = 2 / (30 + 1)   # Slowest EMA smoothing constant

    for ticker in tickers:
        if column not in df[ticker].columns:
            print(f"Warning: '{column}' column not found for {ticker}.")
            continue

        series = df[(ticker, column)]
        price = series.copy()

        # Noise = sum of absolute price changes
        noise = price.diff().abs().rolling(window=window).sum()

        # Signal = absolute net change over window
        signal = (price - price.shift(window)).abs()

        # Efficiency Ratio (ER)
        er = signal / noise.replace(0, 1e-10)  # avoid divide by zero

        # Smoothing constant (SC)
        sc = (er * (fast_alpha - slow_alpha) + slow_alpha) ** 2

        # Initialize KAMA
        kama = pd.Series(index=price.index, dtype=float)
        kama.iloc[window] = price.iloc[window]  # start from first valid point

        for i in range(window + 1, len(price)):
            kama.iloc[i] = kama.iloc[i - 1] + sc.iloc[i] * (price.iloc[i] - kama.iloc[i - 1])

        df[(ticker, f"{column}_KAMA")] = kama

    return df


def run(identifier, df_name, column="close", window=21):
    """
    Retrieve stored data and compute KAMA for the specified column.

    Args:
        identifier (str): Node ID (used for logging or metadata).
        df_name (str): Name key to fetch the correct DataFrame from results_store.
        column (str): Column to compute KAMA on.
        window (int): KAMA window size (default: 21).

    Returns:
        pd.DataFrame or None: Updated DataFrame with KAMA column, if data exists.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty. Check CSV data and columns.")
        return None

    return calculate_kama(df, column, window)
