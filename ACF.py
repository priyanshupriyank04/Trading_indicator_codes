import pandas as pd
import matplotlib.pyplot as plt
import io
import base64
import json
from statsmodels.graphics.tsaplots import plot_acf
from main import get_results_store  # Import function instead of variable


def generate_acf_plot(df, column, lags=20):
    """
    Generate an Autocorrelation Function (ACF) plot for a given column in a multi-level DataFrame 
    and return it as JSON.

    Args:
        df (pd.DataFrame): Multi-level DataFrame containing time-series data.
        column (str): Column name for which ACF should be plotted.
        lags (int, optional): Number of lags to include in the ACF plot (default is 20).

    Returns:
        str: A JSON string containing the base64-encoded ACF plot images.
    """
    tickers = df.columns.levels[0]  # Extract tickers from level 0

    acf_results = {}
    for ticker in tickers:
        if column in df[ticker].columns:  # Check if the specified column exists
            fig, ax = plt.subplots(figsize=(10, 5))
            plot_acf(df[(ticker, column)].dropna(), lags=lags, ax=ax)
            plt.title(f"ACF Plot for {ticker} - {column}")

            # Save the plot to a bytes buffer
            buf = io.BytesIO()
            plt.savefig(buf, format="png")
            buf.seek(0)

            # Encode the image as base64
            image_base64 = base64.b64encode(buf.read()).decode("utf-8")

            # Close the plot to free memory
            plt.close(fig)

            # Store in dictionary with ticker and column name
            acf_results[(ticker, f"{column}_acf")] = image_base64
        else:
            print(f"Warning: '{column}' column not found for {ticker}.")

    # Return as JSON
    return json.dumps(acf_results)


def run(identifier, column="close", lags=20):
    """
    Retrieve stored data and generate ACF plots for the specified column.

    Args:
        identifier (str): Unique identifier to fetch the corresponding DataFrame.
        column (str, optional): Column name to compute ACF on. Defaults to "close".
        lags (int, optional): Number of lags for ACF calculation. Defaults to 20.

    Returns:
        str or None: JSON string containing base64-encoded ACF plots if data exists, else None.
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

    return generate_acf_plot(df, column, lags)
