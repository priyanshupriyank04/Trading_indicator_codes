import pandas as pd
import json
from statsmodels.tsa.stattools import adfuller
from main import get_results_store  # Import function instead of variable


def perform_adf_test(df, column, alpha=0.05):
    """
    Perform the Augmented Dickey-Fuller (ADF) test on a given column in a multi-level DataFrame.

    Args:
        df (pd.DataFrame): Multi-level DataFrame containing time-series data.
        column (str): Column name to test for stationarity.
        alpha (float, optional): Significance level for the test (default is 0.05).

    Returns:
        pd.DataFrame: DataFrame with ADF test results stored under each ticker with 'adf' abbreviation.
    """
    tickers = df.columns.levels[0]  # Extract tickers from level 0 of the MultiIndex columns

    for ticker in tickers:
        if column in df[ticker].columns:  # Check if the specified column exists
            result = adfuller(df[(ticker, column)].dropna())
            p_value = result[1]  # Extract p-value from the ADF test results
            is_stationary = p_value < alpha  # Determine stationarity
            df[(ticker, f"{column}_adf")] = is_stationary  # Store result as a new column
        else:
            print(f"Warning: '{column}' column not found for {ticker}.")

    return df


def run(identifier, column="close", alpha=0.05):
    """
    Retrieve stored data and perform the ADF test on the specified column.

    Args:
        identifier (str): Unique identifier to fetch the corresponding DataFrame.
        column (str, optional): Column name to perform the ADF test on. Defaults to "close".
        alpha (float, optional): Significance level for the ADF test. Defaults to 0.05.

    Returns:
        pd.DataFrame or None: DataFrame with ADF test results added if data exists, else None.
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

    return perform_adf_test(df, column, alpha)
