import pandas as pd
from main import get_results_store


def calculate_stochrsi(df, column='close', rsi_length=14, stoch_length=14, k_smooth=3, d_smooth=3):
    """
    Calculate the Stochastic RSI (%K and %D) for each ticker in a multi-index DataFrame.

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.
        column (str): Column to calculate RSI from (usually 'close').
        rsi_length (int): Period for RSI calculation.
        stoch_length (int): Lookback period for StochRSI calculation.
        k_smooth (int): Smoothing period for %K line.
        d_smooth (int): Smoothing period for %D line.

    Returns:
        pd.DataFrame: DataFrame with StochRSI %K and %D columns added per ticker.
    """
    tickers = df.columns.levels[0]

    for ticker in tickers:
        if column not in df[ticker].columns:
            print(f"Missing '{column}' column for {ticker}, skipping...")
            continue

        close = df[(ticker, column)]

        # RSI calculation
        delta = close.diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)

        avg_gain = gain.ewm(alpha=1 / rsi_length, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1 / rsi_length, adjust=False).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        # Stochastic RSI calculation
        min_rsi = rsi.rolling(window=stoch_length).min()
        max_rsi = rsi.rolling(window=stoch_length).max()
        stoch_rsi = (rsi - min_rsi) / (max_rsi - min_rsi)

        # %K and %D
        k = stoch_rsi.rolling(window=k_smooth).mean()
        d = k.rolling(window=d_smooth).mean()

        df[(ticker, f"StochRSI_K_{rsi_length}_{stoch_length}")] = k
        df[(ticker, f"StochRSI_D_{rsi_length}_{stoch_length}")] = d

    return df


def run(identifier, df_name, column='close', rsi_length=14, stoch_length=14, k_smooth=3, d_smooth=3):
    """
    Run Stochastic RSI calculation.

    Args:
        identifier (str): Node ID for logging or metadata.
        df_name (str): Key to retrieve DataFrame from results_store.
        column (str): Column for price data.
        rsi_length (int): RSI period.
        stoch_length (int): Lookback period for StochRSI.
        k_smooth (int): Smoothing period for %K.
        d_smooth (int): Smoothing period for %D.

    Returns:
        pd.DataFrame or None: Updated DataFrame with StochRSI %K and %D columns, or None on failure.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty.")
        return None

    return calculate_stochrsi(df, column, rsi_length, stoch_length, k_smooth, d_smooth)
