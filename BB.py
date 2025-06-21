import pandas as pd
from main import get_results_store

def calculate_bollinger_bands(df, column='close', window=20, mult=2.0, ma_type='SMA'):
    """
    Calculate Bollinger Bands for a given column in a multi-index DataFrame.

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.
        column (str): Price column to calculate BB on ('close' typically).
        window (int): Lookback period for MA and StdDev.
        mult (float): Multiplier for standard deviation.
        ma_type (str): Type of moving average ('SMA', 'EMA', 'RMA', 'WMA', 'VWMA').

    Returns:
        pd.DataFrame: DataFrame with Bollinger Bands columns added.
    """
    tickers = df.columns.levels[0]

    for ticker in tickers:
        if column not in df[ticker].columns:
            print(f"Missing column '{column}' for {ticker}, skipping...")
            continue

        price_series = df[(ticker, column)]

        # Calculate Moving Average
        ma_type = ma_type.upper()
        if ma_type == 'SMA':
            basis = price_series.rolling(window=window).mean()
        elif ma_type == 'EMA':
            basis = price_series.ewm(span=window, adjust=False).mean()
        elif ma_type in ['RMA', 'SMMA']:
            basis = price_series.ewm(alpha=1 / window, adjust=False).mean()
        elif ma_type == 'WMA':
            weights = pd.Series(range(1, window + 1))
            basis = price_series.rolling(window).apply(lambda x: (x * weights).sum() / weights.sum(), raw=True)
        elif ma_type == 'VWMA':
            try:
                volume_series = df[(ticker, 'volume')]
                vwma = (
                    (price_series * volume_series)
                    .rolling(window=window)
                    .sum() /
                    volume_series.rolling(window=window).sum()
                )
                basis = vwma
            except KeyError:
                print(f"Missing 'volume' data for {ticker}, cannot calculate VWMA. Skipping...")
                continue
        else:
            print(f"Unknown MA type '{ma_type}' for {ticker}, skipping...")
            continue

        std = price_series.rolling(window=window).std()

        upper = basis + mult * std
        lower = basis - mult * std

        df[(ticker, f"BB_basis_{window}_{ma_type}")] = basis
        df[(ticker, f"BB_upper_{window}_{ma_type}")] = upper
        df[(ticker, f"BB_lower_{window}_{ma_type}")] = lower

    return df


def run(identifier, df_name, column='close', window=20, mult=2.0, ma_type='SMA'):
    """
    Run Bollinger Bands calculation.

    Args:
        identifier (str): Node ID for logging or metadata.
        df_name (str): Key to retrieve DataFrame from results_store.
        column (str): Column to apply Bollinger Bands on.
        window (int): Lookback period.
        mult (float): StdDev multiplier.
        ma_type (str): Type of moving average.

    Returns:
        pd.DataFrame or None: DataFrame with BB columns, or None on failure.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty.")
        return None

    return calculate_bollinger_bands(df, column, window, mult, ma_type)
