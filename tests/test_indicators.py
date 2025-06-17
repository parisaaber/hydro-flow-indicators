# test_indicators.py
import unittest
import pandas as pd
from raven_api.indicators import (
    mean_annual_flow,
    mean_aug_sep_flow,
    peak_flow_timing,
    days_below_efn,
    peak_flows,
    annual_peaks,
    fit_ffa
)

class TestIndicators(unittest.TestCase):

    def setUp(self):
        # Sample dataset
        self.df = pd.DataFrame({
            'date': pd.to_datetime([
                '2000-08-01', '2000-08-02', '2000-09-01', '2000-10-01',
                '2001-08-01', '2001-09-01', '2001-10-01'
            ]),
            'value': [1, 2, 3, 4, 2, 3, 5],
            'site': ['A', 'A', 'A', 'A', 'A', 'A', 'A']
        })
        self.df['month'] = self.df['date'].dt.month
        self.df['year'] = self.df['date'].dt.year
        self.df['water_year'] = self.df['year']
        self.df.loc[self.df['month'] >= 10, 'water_year'] += 1

    def test_mean_annual_flow(self):
        result = mean_annual_flow(self.df)
        self.assertIn('A', result.index)
        self.assertGreater(result['A'], 0)

    def test_mean_aug_sep_flow(self):
        result = mean_aug_sep_flow(self.df)
        self.assertAlmostEqual(result['A'], 2.2, places=1)

    def test_peak_flow_timing(self):
        result = peak_flow_timing(self.df)
        self.assertIn('A', result.index)
        self.assertTrue(200 <= result['A'] <= 366)

    def test_days_below_efn(self):
        result = days_below_efn(self.df, EFN_threshold=0.5)
        self.assertIn('A', result.index)
        self.assertIsInstance(result['A'], float)

    def test_peak_flows(self):
        result = peak_flows(self.df)
        self.assertAlmostEqual(result['A'], 4.0, places=1)  # ✅ Corrected expected value

    def test_annual_peaks(self):
        result = annual_peaks(self.df)
        self.assertIn('value', result.columns)
        self.assertGreaterEqual(result['value'].max(), 5)

    def test_fit_ffa(self):
        peaks = annual_peaks(self.df)
        result = fit_ffa(peaks, return_periods=[2, 20])
        self.assertIn('Q2', result.columns)
        self.assertIn('Q20', result.columns)

if __name__ == '__main__':
    unittest.main()

