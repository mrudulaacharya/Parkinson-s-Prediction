import pandas as pd
import numpy as np

def handle_duplicates(df, subset=None, keep='first', inplace=False, ignore_index=False):
    """
    Detects and removes duplicate records from the dataset.

    Parameters:
    df (pandas.DataFrame): Input dataset
    subset (list): Columns to consider for identifying duplicates. If None, use all columns.
    keep (str): 'first' keeps the first occurrence, 'last' keeps the last occurrence, False drops all duplicates.
    inplace (bool): If True, do operation inplace and return None.
    ignore_index (bool): If True, the resulting axis will be labeled 0, 1, …, n - 1.

    Returns:
    pandas.DataFrame: Cleaned dataset with duplicates removed
    """
    # Count duplicates before removal
    duplicate_count = df.duplicated(subset=subset).sum()
    print(f"Number of duplicate records detected: {duplicate_count}")

    # Remove duplicates
    cleaned_df = df.drop_duplicates(subset=subset, keep=keep, inplace=inplace, ignore_index=ignore_index)

    # Count records after removal
    records_removed = len(df) - len(cleaned_df)
    print(f"Number of records removed: {records_removed}")

    return cleaned_df

def analyze_duplicates(df, subset=None):
    """
    Analyzes duplicate records in the dataset.

    Parameters:
    df (pandas.DataFrame): Input dataset
    subset (list): Columns to consider for identifying duplicates. If None, use all columns.

    Returns:
    pandas.DataFrame: DataFrame containing information about duplicate records
    """
    # Identify duplicate records
    duplicates = df[df.duplicated(subset=subset, keep=False)]

    # Group duplicates and count occurrences
    duplicate_counts = duplicates.groupby(subset).size().reset_index(name='count')
    duplicate_counts = duplicate_counts.sort_values('count', ascending=False)

    print("Top 10 most frequent duplicate records:")
    print(duplicate_counts.head(10))

    return duplicates

if __name__ == "__main__":
    # Load the dataset
    file_path = 'merged__m_output.csv'
    dataset = pd.read_csv(file_path)

    print("Original dataset shape:", dataset.shape)

    # Analyze duplicates
    duplicate_analysis = analyze_duplicates(dataset)

    # Handle duplicates
    cleaned_dataset = handle_duplicates(dataset, subset=None, keep='first', ignore_index=True)

    print("\nCleaned dataset shape:", cleaned_dataset.shape)

    # Save the cleaned dataset
    cleaned_file_path = 'cleaned_dataset_no_duplicates.csv'
    cleaned_dataset.to_csv(cleaned_file_path, index=False)
    print(f"\nCleaned dataset saved as '{cleaned_file_path}'")

    # Display summary statistics of the cleaned dataset
    print("\nSummary statistics of the cleaned dataset:")
    print(cleaned_dataset.describe())

    # Check for any remaining duplicates
    remaining_duplicates = cleaned_dataset.duplicated().sum()
    print(f"\nRemaining duplicates after cleaning: {remaining_duplicates}")