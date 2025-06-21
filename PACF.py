import pandas as pd
import matplotlib.pyplot as plt
import io
import base64
import json
from statsmodels.graphics.tsaplots import plot_pacf
from main import get_results_store  # Import function instead of variable


def generate_pacf_plot(df, column, lags=20):
    """
    Generate a Partial Autocorrelation Function (PACF) plot for a given column in a multi-level DataFrame 
    and return it as a JSON string containing base64-encoded PACF plot images.

    Args:
        df (pd.DataFrame): Multi-level DataFrame containing time-series data.
        column (str): The column name for which PACF should be plotted.
        lags (int, optional): The number of lags to include in the PACF plot. Defaults to 20.

    Returns:
        str: A JSON string containing the base64-encoded PACF plot images.
    """
    tickers = df.columns.levels[0]  # Extract tickers from level 0 of the MultiIndex columns

    plots = {}
    for ticker in tickers:
        if column in df[ticker].columns:
            fig, ax = plt.subplots(figsize=(10, 5))
            plot_pacf(df[(ticker, column)].dropna(), lags=lags, method="ywm", ax=ax)
            plt.title(f"PACF Plot for {ticker} - {column}")

            # Save the plot to a bytes buffer
            buf = io.BytesIO()
            plt.savefig(buf, format="png")
            buf.seek(0)

            # Encode the image as base64
            image_base64 = base64.b64encode(buf.read()).decode("utf-8")

            # Close the plot to free memory
            plt.close(fig)

            # Store the image in a dictionary with an appropriate key
            plots[f"{ticker}_{column}_pacf"] = image_base64
        else:
            print(f"Warning: '{column}' column not found for {ticker}.")

    return json.dumps(plots)


def run(identifier, column="close", lags=20):
    """
    Retrieve stored data and generate PACF plots for the specified column.

    Args:
        identifier (str): Unique identifier to fetch the corresponding DataFrame.
        column (str, optional): Column name to compute PACF on. Defaults to "close".
        lags (int, optional): Number of lags for PACF calculation. Defaults to 20.

    Returns:
        str or None: JSON string with base64-encoded PACF plots if data exists, else None.
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

    return generate_pacf_plot(df, column, lags)
