import pandas as pd
from main import get_results_store

def calculate_true_range(df):
    high = df['high']
    low = df['low']
    close = df['close'].shift(1)

    tr1 = high - low
    tr2 = (high - close).abs()
    tr3 = (low - close).abs()

    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr

def calculate_atr(df, window, smoothing='RMA'):
    tickers = df.columns.levels[0]

    for ticker in tickers:
        try:
            high = df[(ticker, 'high')]
            low = df[(ticker, 'low')]
            close = df[(ticker, 'close')]
        except KeyError:
            print(f"Missing columns for {ticker}, skipping...")
            continue

        tr = calculate_true_range(df[ticker])

        if smoothing.upper() == 'EMA':
            atr = tr.ewm(span=window, adjust=False).mean()
        elif smoothing.upper() == 'SMA':
            atr = tr.rolling(window=window).mean()
        elif smoothing.upper() == 'WMA':
            weights = pd.Series(range(1, window + 1))
            atr = tr.rolling(window).apply(lambda x: (x * weights).sum() / weights.sum(), raw=True)
        else:  # Default to RMA (Wilder’s)
            atr = tr.ewm(alpha=1/window, adjust=False).mean()

        df[(ticker, f'ATR_{smoothing.upper()}')] = atr

    return df


def run(identifier, df_name, window=14, smoothing='RMA'):
    results_store = get_results_store()

    if df_name not in results_store:
        print(f"Error: No data found for df_name '{df_name}' in results_store.")
        return None

    df = results_store[df_name]

    if df is None or df.empty:
        print(f"Error: DataFrame '{df_name}' is empty.")
        return None

    return calculate_atr(df, window, smoothing)
