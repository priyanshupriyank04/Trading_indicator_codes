import pandas as pd
from main import get_results_store


def rma(series, window):
    """
    Wilder's smoothing (Recursive Moving Average) equivalent to Pine Script's ta.rma().
    """
    alpha = 1 / window
    return series.ewm(alpha=alpha, adjust=False).mean()


def calculate_adx(df, di_len, adx_len):
    """
    Calculate the Average Directional Index (ADX) for each ticker in a multi-index DataFrame.

    Args:
        df (pd.DataFrame): Multi-index DataFrame with tickers as level 0.
        di_len (int): DI calculation window length.
        adx_len (int): ADX smoothing length.

    Returns:
        pd.DataFrame: Updated DataFrame with ADX column for each ticker.
    """
    tickers = df.columns.levels[0]

    for ticker in tickers:
        columns = df[ticker].columns
        if not all(col in columns for col in ['high', 'low', 'close']):
            print(f"Warning: Required OHLC columns not found for {ticker}.")
            continue

        high = df[(ticker, 'high')]
        low = df[(ticker, 'low')]
        close = df[(ticker, 'close')]

        # Step 1: True Range
        prev_close = close.shift(1)
        high_low = high - low
        high_close = (high - prev_close).abs()
        low_close = (low - prev_close).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)

        # Step 2: Directional Movement
        up_move = high.diff()
        down_move = -low.diff()

        plus_dm = ((up_move > down_move) & (up_move > 0)) * up_move
        minus_dm = ((down_move > up_move) & (down_move > 0)) * down_move

        # Step 3: RMA of TR and DMs
        tr_rma = rma(tr, di_len)
        plus_dm_rma = rma(plus_dm, di_len)
        minus_dm_rma = rma(minus_dm, di_len)

        # Step 4: +DI and -DI
        plus_di = 100 * (plus_dm_rma / tr_rma)
        minus_di = 100 * (minus_dm_rma / tr_rma)

        # Step 5: DX
        dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, 1)  # Avoid division by zero

        # Step 6: ADX
        adx = rma(dx, adx_len)

        # Store ADX in DataFrame
        df[(ticker, 'ADX')] = adx

    return df


def run(identifier, df_name, di_len=14, adx_len=14):
    """
    Retrieve stored data and compute the ADX for each ticker.

    Args:
        identifier (str): Node ID for tracking/logging.
        df_name (str): Key to fetch the correct DataFrame.
        di_len (int): DI window (default: 14).
        adx_len (int): ADX smoothing window (default: 14).

    Returns:
        pd.DataFrame or None: Updated DataFrame with ADX column.
    """
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty.")
        return None

    return calculate_adx(df, di_len, adx_len)
