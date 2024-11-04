import unittest
import pandas as pd
import numpy as np
from missing_values_handler import handle_missing_values

class TestMissingValuesHandler(unittest.TestCase):

    def setUp(self):
        # Create a sample dataset with various types of missing values
        self.data = pd.DataFrame({
            'AV133STDY': [1.0, np.nan, 3.0, np.nan, 5.0],
            'TAUSTDY': [np.nan, 2.0, np.nan, 4.0, 5.0],
            'GAITSTDY': [1.0, 2.0, 3.0, 4.0, np.nan],
            'PISTDY': [np.nan, np.nan, np.nan, np.nan, np.nan],
            'NP4DYSTN': [1, 2, np.nan, 4, 5],
            'NP4TOT': [10, np.nan, 30, np.nan, 50]
        })

    def test_missing_values_identification(self):
        # Test if the function correctly identifies missing values
        missing_values = handle_missing_values(self.data, identify_only=True)

        self.assertEqual(missing_values['AV133STDY'], 2)
        self.assertEqual(missing_values['TAUSTDY'], 2)
        self.assertEqual(missing_values['GAITSTDY'], 1)
        self.assertEqual(missing_values['PISTDY'], 5)
        self.assertEqual(missing_values['NP4DYSTN'], 1)
        self.assertEqual(missing_values['NP4TOT'], 2)

    def test_missing_values_handling(self):
        # Test if the function correctly handles missing values
        handled_data = handle_missing_values(self.data)

        # Check if numerical columns are imputed with mean
        self.assertAlmostEqual(handled_data['AV133STDY'].mean(), 3.0)
        self.assertAlmostEqual(handled_data['TAUSTDY'].mean(), 3.75)
        self.assertAlmostEqual(handled_data['GAITSTDY'].mean(), 2.5)

        # Check if columns with all missing values are dropped
        self.assertNotIn('PISTDY', handled_data.columns)

        # Check if categorical columns are imputed with mode
        self.assertTrue(handled_data['NP4DYSTN'].notna().all())

        # Check if NP4TOT is imputed correctly
        self.assertTrue(handled_data['NP4TOT'].notna().all())

    def test_threshold_dropping(self):
        # Test if columns with more than 50% missing values are dropped
        handled_data = handle_missing_values(self.data, drop_threshold=0.5)

        self.assertNotIn('PISTDY', handled_data.columns)
        self.assertIn('AV133STDY', handled_data.columns)

    def test_custom_strategy(self):
        # Test if custom imputation strategy works
        custom_strategy = {
            'AV133STDY': 'median',
            'TAUSTDY': 'constant',
            'GAITSTDY': lambda x: x.fillna(x.min())
        }
        handled_data = handle_missing_values(self.data, strategy=custom_strategy, constant_value=100)

        self.assertEqual(handled_data['AV133STDY'].median(), 3.0)
        self.assertEqual(handled_data['TAUSTDY'].fillna(100).unique().tolist(), [100.0, 2.0, 4.0, 5.0])
        self.assertEqual(handled_data['GAITSTDY'].min(), 1.0)

    def test_error_handling(self):
        # Test if the function raises appropriate errors
        with self.assertRaises(ValueError):
            handle_missing_values(self.data, strategy='invalid_strategy')

        with self.assertRaises(TypeError):
            handle_missing_values('not_a_dataframe')

if __name__ == '__main__':
    unittest.main()