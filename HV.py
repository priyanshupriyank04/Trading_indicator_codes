import numpy as np
import pandas as pd
from main import get_results_store  # Import function to access stored data


def calculate_hv(df, column, window, annual=365, period=1):
    """
    Calculate the Historical Volatility (HV) based on standard deviation of log returns.

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.
        column (str): The column name (usually 'close') to compute HV from.
        window (int): Rolling window for standard deviation of log returns.
        annual (int): Annualization factor (default: 365).
        period (int): Base period multiplier (1 for daily/intraday, 7 for weekly etc.).

    Returns:
        pd.DataFrame: Updated DataFrame with HV column added per ticker.
    """
    tickers = df.columns.levels[0]

    for ticker in tickers:
        if column not in df[ticker].columns:
            print(f"Warning: '{column}' column not found for {ticker}.")
            continue

        price_series = df[(ticker, column)]
        log_returns = np.log(price_series / price_series.shift(1))
        rolling_std = log_returns.rolling(window).std()
        hv = 100 * rolling_std * np.sqrt(annual / period)

        df[(ticker, f"{column}_HV")] = hv

    return df


def run(identifier, df_name, column="close", window=10):
    """
    Retrieve stored data and compute Historical Volatility (HV) for the specified column.

    Args:
        identifier (str): Node ID for logging or metadata.
        df_name (str): Name key to fetch the correct DataFrame from results_store.
        column (str): Column to compute HV on (default: 'close').
        window (int): Lookback period for log return volatility (default: 10).

    Returns:
        pd.DataFrame or None: DataFrame with HV column added, or None if input is invalid.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty. Check CSV data and columns.")
        return None

    return calculate_hv(df, column, window)
