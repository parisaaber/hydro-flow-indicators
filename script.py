from scipy.stats import gumbel_r
import pandas as pd
import numpy as np
import duckdb

def load_raven_output(csv_path):
    df = pd.read_csv(csv_path, sep='\t|,', engine='python')
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.drop(columns=['time', 'hour'], errors='ignore')
    df = df.set_index('date')
    df = df.loc[:, ~df.columns.str.contains('observed', case=False)]
    return df

def reshape_to_long(df, exclude_precip=True):
    df = df.reset_index()
    if exclude_precip:
        precip_cols = [col for col in df.columns if 'precip' in col.lower()]
        df = df.drop(columns=precip_cols, errors='ignore')
    long_df = df.melt(id_vars='date', var_name='site', value_name='value')
    long_df['value'] = pd.to_numeric(long_df['value'], errors='coerce')
    return long_df.dropna(subset=['value'])

def save_to_parquet(df, out_path):
    df.to_parquet(out_path)
    print(f"✅ Saved to {out_path}")

def calculate_indicators(parquet_path, group_id=['site', 'water_year'], EFN_threshold=0.2, break_point=None):
    def get_annual_peaks(df):
        df['year'] = df['date'].dt.year
        df['water_year'] = np.where(df['date'].dt.month >= 10, df['year'] + 1, df['year'])
        return df.groupby(['site', 'water_year', 'subperiod'])['value'].max().reset_index(name='annual_max')
    def fit_ffa(df, dist='gumbel', return_periods=[2, 20]):
        result = {}
        values = df['annual_max'].dropna()
        if len(values) < 2:
            return {rp: np.nan for rp in return_periods}
        if dist == 'gumbel':
            loc, scale = gumbel_r.fit(values)
            for rp in return_periods:
                prob = 1 - 1/rp
                quantile = gumbel_r.ppf(prob, loc=loc, scale=scale)
                result[rp] = quantile
        else:
            raise NotImplementedError(f"Distribution {dist} not supported")
        return result
    con = duckdb.connect()
    query = f"""
        SELECT date, value, site
        FROM parquet_scan('{parquet_path}')
        WHERE value IS NOT NULL
    """
    df = con.execute(query).fetchdf()
    df['date'] = pd.to_datetime(df['date'])
    df['month'] = df['date'].dt.month
    df['year'] = df['date'].dt.year
    df['water_year'] = np.where(df['month'] >= 10, df['year'] + 1, df['year'])
    if break_point is not None:
        df['subperiod'] = np.where(df['water_year'] <= break_point,
                                   f"before_{break_point}",
                                   f"after_{break_point}")
    else:
        df['subperiod'] = 'full_period'
    mean_annual_flow = df.groupby(['site', 'subperiod'])['value'].mean().reset_index(name='Mean Annual Flow')
    df_aug_sep = df[df['month'].isin([8, 9])]
    mean_aug_sep_flow = df_aug_sep.groupby(['site', 'subperiod'])['value'].mean().reset_index(name='Mean Aug-Sep Flow')
    def peak_flow_timing(group):
        idx_max = group['value'].idxmax()
        peak_date = group.loc[idx_max, 'date']
        return peak_date.timetuple().tm_yday
    peak_flow_timing_df = df.groupby(['site', 'subperiod']).apply(lambda g: peak_flow_timing(g)).reset_index(name='Peak Flow Timing')
    def days_below_efn(group):
        mean_flow = group['value'].mean()
        threshold = EFN_threshold * mean_flow
        return (group['value'] < threshold).sum()
    def days_below_efn_per_year(group):
      mean_flow = group['value'].mean()
      threshold = EFN_threshold * mean_flow
      days_below = (group['value'] < threshold).sum()
      return days_below
    days_below_df = df.groupby(['site', 'subperiod', 'water_year']).apply(days_below_efn_per_year).reset_index(name='Days below EFN')
    days_below_avg = days_below_df.groupby(['site', 'subperiod'])['Days below EFN'].mean().reset_index()
    days_below_efn_df = round(days_below_df.groupby(['site', 'subperiod'])['Days below EFN'].mean().reset_index())
    annual_peaks = get_annual_peaks(df)
    def ffa_per_site_subperiod(group):
        return_periods = [2, 20]
        result = fit_ffa(group, return_periods=return_periods)
        return pd.Series({
            '2-Year Peak Flow': result.get(2, np.nan),
            '20-Year Peak Flow': result.get(20, np.nan)
        })
    peak_flows_merged = annual_peaks.groupby(['site', 'subperiod']).apply(ffa_per_site_subperiod).reset_index()
    merged = mean_annual_flow.merge(mean_aug_sep_flow, on=['site', 'subperiod'], how='left') \
                             .merge(peak_flow_timing_df, on=['site', 'subperiod'], how='left') \
                             .merge(days_below_efn_df, on=['site', 'subperiod'], how='left') \
                             .merge(peak_flows_merged, on=['site', 'subperiod'], how='left')
    cols = [
        'site', 'subperiod',
        'Days below EFN',
        '2-Year Peak Flow',
        'Mean Annual Flow',
        'Mean Aug-Sep Flow',
        'Peak Flow Timing',
        '20-Year Peak Flow'
    ]
    return merged[cols]
