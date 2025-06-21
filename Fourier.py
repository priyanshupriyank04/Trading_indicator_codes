import pandas as pd
import numpy as np
from main import get_results_store  # Import function instead of variable


def apply_fourier_forecast(df, column, forecast_length, window_size=100):
    """
    Applies Fourier Transformation to forecast future values based on the specified column for each ticker
    in a multi-level DataFrame.

    Args:
        df (pd.DataFrame): Multi-level input DataFrame containing time-series data.
        column (str): Column name to apply forecasting on.
        forecast_length (int): Number of future periods to forecast.
        window_size (int, optional): Rolling window size for frequency extraction. Defaults to 100.

    Returns:
        pd.DataFrame: DataFrame with projected returns and forecasted values added under each ticker.
    """
    tickers = df.columns.levels[0]  # Extract tickers from level 0
    forecasts = {}

    for ticker in tickers:
        if column in df[ticker].columns:
            df_ticker = df[ticker].dropna(subset=[column]).copy()

            # Compute log returns and drop NaN values
            df_ticker["returns"] = df_ticker[column].pct_change()
            df_ticker = df_ticker.dropna()

            returns = df_ticker["returns"].values
            N = len(returns)

            significant_freqs = set()
            num_windows = max(1, N - window_size + 1)

            # Identify significant frequency components via FFT
            for i in range(num_windows):
                window_returns = returns[i : i + window_size]
                window = np.hanning(window_size)
                windowed_returns = window_returns * window

                fft_coeffs = np.fft.fft(windowed_returns)
                magnitudes = np.abs(fft_coeffs)

                cutoff = int(0.15 * window_size)
                top_indices = np.argsort(magnitudes)[-cutoff:]
                significant_freqs.update(top_indices)

            # Apply FFT to the full return series
            full_window = np.hanning(N)
            full_fft = np.fft.fft(returns * full_window)

            # Filter only significant frequencies
            filtered_fft = np.zeros_like(full_fft)
            for freq in significant_freqs:
                if freq < len(full_fft):
                    filtered_fft[freq] = full_fft[freq]

            # Reconstruct the signal using IFFT
            reconstructed = np.fft.ifft(filtered_fft).real / full_window.mean()

            # Generate future returns by iterating over reconstructed values
            future_returns = []
            current_pos = max(0, len(reconstructed) - window_size)
            stride = window_size // 2

            while len(future_returns) < forecast_length:
                segment = reconstructed[current_pos : current_pos + window_size]
                if len(segment) < window_size:
                    segment = np.pad(segment, (0, window_size - len(segment)), "constant")
                future_returns.extend(segment)
                current_pos = max(0, current_pos - stride)

            future_returns = np.array(future_returns[:forecast_length])

            # Adjust future returns variance to match historical data
            hist_std = returns.std()
            future_std = future_returns.std()
            if future_std > 1e-6:
                future_returns *= hist_std / future_std

            # Add noise to the returns to introduce realistic uncertainty
            noise = np.random.normal(0, hist_std * 0.2, forecast_length)
            future_returns += noise * np.linspace(1, 0.3, forecast_length)

            # Compute forecasted price series
            last_close = df_ticker[column].iloc[-1]
            future_prices = last_close * np.exp(np.cumsum(future_returns))

            # Generate future index range
            future_indices = range(df_ticker.index[-1] + 1, df_ticker.index[-1] + forecast_length + 1)

            # Store forecast results
            forecasts[ticker] = pd.DataFrame(
                {
                    (ticker, f"{column}_projected_returns"): future_returns,
                    (ticker, f"{column}_projected_close"): future_prices,
                },
                index=future_indices,
            )

    # Concatenate forecast results if available
    df_future = pd.concat(forecasts.values(), axis=1) if forecasts else pd.DataFrame()
    return df_future


def run(identifier, column="close", forecast_length=30, window_size=100):
    """
    Retrieve stored data and compute Fourier-based forecasting.

    Args:
        identifier (str): Unique identifier to fetch the corresponding DataFrame.
        column (str, optional): Column name to apply Fourier forecast. Defaults to "close".
        forecast_length (int, optional): Number of future periods to forecast. Defaults to 30.
        window_size (int, optional): Window size for Fourier frequency extraction. Defaults to 100.

    Returns:
        pd.DataFrame or None: DataFrame with forecasted values if data exists, else None.
    """
    results_store = get_results_store()  # Retrieve stored results
    stored_key = f"{identifier}"  # Ensure correct key format

    if stored_key not in results_store:
        print(f"Error: No data found for identifier '{stored_key}' in results_store.")
        return None

    df = results_store[stored_key]

    if df is None or df.empty:
        print(f"Error: DataFrame '{stored_key}' is empty. Check CSV data and columns.")
        return None

    return apply_fourier_forecast(df, column, forecast_length, window_size)
