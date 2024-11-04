import unittest
import pandas as pd
import io
from duplicates_handler import handle_duplicates, analyze_duplicates

class TestDuplicatesHandler(unittest.TestCase):

    def setUp(self):
        # Create a sample dataset with duplicates
        self.data = pd.DataFrame({
            'ID': [1, 2, 3, 2, 4, 5, 1, 6],
            'Name': ['John', 'Jane', 'Bob', 'Jane', 'Alice', 'Charlie', 'John', 'David'],
            'Age': [30, 25, 35, 25, 28, 32, 30, 27],
            'City': ['New York', 'London', 'Paris', 'London', 'Berlin', 'Tokyo', 'New York', 'Sydney']
        })

    def test_handle_duplicates_all_columns(self):
        cleaned_df = handle_duplicates(self.data)
        self.assertEqual(len(cleaned_df), 6)  # 2 duplicates should be removed
        self.assertFalse(cleaned_df.duplicated().any())

    def test_handle_duplicates_subset(self):
        cleaned_df = handle_duplicates(self.data, subset=['Name', 'Age'])
        self.assertEqual(len(cleaned_df), 7)  # 1 duplicate should be removed
        self.assertFalse(cleaned_df.duplicated(subset=['Name', 'Age']).any())

    def test_handle_duplicates_keep_last(self):
        cleaned_df = handle_duplicates(self.data, keep='last')
        self.assertEqual(len(cleaned_df), 6)
        self.assertEqual(cleaned_df.iloc[-1]['Name'], 'John')  # Last 'John' should be kept

    def test_handle_duplicates_drop_all(self):
        cleaned_df = handle_duplicates(self.data, keep=False)
        self.assertEqual(len(cleaned_df), 4)  # All rows involved in duplication should be removed

    def test_analyze_duplicates(self):
        duplicates = analyze_duplicates(self.data, subset=['Name'])
        self.assertEqual(len(duplicates), 4)  # 4 rows should be identified as duplicates

    def test_analyze_duplicates_no_subset(self):
        duplicates = analyze_duplicates(self.data)
        self.assertEqual(len(duplicates), 4)  # 4 rows should be identified as duplicates

    def test_handle_duplicates_inplace(self):
        df_copy = self.data.copy()
        handle_duplicates(df_copy, inplace=True)
        self.assertEqual(len(df_copy), 6)  # 2 duplicates should be removed inplace

    def test_handle_duplicates_ignore_index(self):
        cleaned_df = handle_duplicates(self.data, ignore_index=True)
        self.assertEqual(cleaned_df.index.tolist(), list(range(6)))  # Index should be reset

    def test_empty_dataframe(self):
        empty_df = pd.DataFrame()
        cleaned_df = handle_duplicates(empty_df)
        self.assertTrue(cleaned_df.empty)

    def test_no_duplicates(self):
        no_dup_df = pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']})
        cleaned_df = handle_duplicates(no_dup_df)
        self.assertEqual(len(cleaned_df), 3)
        self.assertFalse(cleaned_df.duplicated().any())

if __name__ == '__main__':
    unittest.main()