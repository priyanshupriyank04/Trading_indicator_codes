import pandas as pd
from main import get_results_store


def calculate_macd(df, column='close', fast_length=12, slow_length=26, signal_length=9,
                   ma_type='EMA', signal_ma_type='EMA'):
    """
    Calculate the Moving Average Convergence Divergence (MACD) for each ticker in a multi-index DataFrame.

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.
        column (str): Column for price (usually 'close').
        fast_length (int): Period for fast moving average.
        slow_length (int): Period for slow moving average.
        signal_length (int): Period for signal line moving average.
        ma_type (str): Type of MA for MACD line ('SMA', 'EMA').
        signal_ma_type (str): Type of MA for signal line ('SMA', 'EMA').

    Returns:
        pd.DataFrame: DataFrame with MACD, signal, and histogram columns added per ticker.
    """
    tickers = df.columns.levels[0]

    for ticker in tickers:
        if column not in df[ticker].columns:
            print(f"Missing '{column}' column for {ticker}, skipping...")
            continue

        price = df[(ticker, column)]
        ma_type = ma_type.upper()
        signal_ma_type = signal_ma_type.upper()

        if ma_type == 'SMA':
            fast_ma = price.rolling(window=fast_length).mean()
            slow_ma = price.rolling(window=slow_length).mean()
        elif ma_type == 'EMA':
            fast_ma = price.ewm(span=fast_length, adjust=False).mean()
            slow_ma = price.ewm(span=slow_length, adjust=False).mean()
        else:
            print(f"Unsupported MA type '{ma_type}' for MACD, skipping {ticker}...")
            continue

        macd_line = fast_ma - slow_ma

        if signal_ma_type == 'SMA':
            signal_line = macd_line.rolling(window=signal_length).mean()
        elif signal_ma_type == 'EMA':
            signal_line = macd_line.ewm(span=signal_length, adjust=False).mean()
        else:
            print(f"Unsupported Signal MA type '{signal_ma_type}' for MACD, skipping {ticker}...")
            continue

        histogram = macd_line - signal_line

        df[(ticker, f"MACD_{fast_length}_{slow_length}_{ma_type}")] = macd_line
        df[(ticker, f"MACD_SIGNAL_{signal_length}_{signal_ma_type}")] = signal_line
        df[(ticker, f"MACD_HIST_{fast_length}_{slow_length}_{signal_length}")] = histogram

    return df


def run(identifier, df_name, column='close', fast_length=12, slow_length=26, signal_length=9,
        ma_type='EMA', signal_ma_type='EMA'):
    """
    Run MACD calculation.

    Args:
        identifier (str): Node ID for logging or metadata.
        df_name (str): Key to retrieve DataFrame from results_store.
        column (str): Column for price data.
        fast_length (int): Fast MA window.
        slow_length (int): Slow MA window.
        signal_length (int): Signal line window.
        ma_type (str): MA type for MACD.
        signal_ma_type (str): MA type for signal line.

    Returns:
        pd.DataFrame or None: Updated DataFrame with MACD, signal, and histogram columns, or None on failure.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty.")
        return None

    return calculate_macd(df, column, fast_length, slow_length, signal_length, ma_type, signal_ma_type)
