import os
import datetime

import pandas as pd
import numpy as np


def process_volatility_file(config):
    """Processes volatility file data from OMI website."""

    data_folder = config.data_folder
    csv_path = os.path.join(data_folder, config.source_data_csv_path)
    df = pd.read_csv(csv_path, index_col=0)  # no explicit index

    # Adds additional date/day fields
    idx = [str(s).split('+')[0] for s in df.index]  # ignore timezones, we don't need them
    dates = pd.to_datetime(idx)
    df['date'] = dates
    df['days_from_start'] = (dates - datetime.datetime(2000, 1, 3)).days
    df['day_of_week'] = dates.dayofweek
    df['day_of_month'] = dates.day
    df['week_of_year'] = dates.weekofyear
    df['month'] = dates.month
    df['year'] = dates.year
    df['categorical_id'] = df['Symbol'].copy()

    # Processes log volatility
    vol = df['rv5_ss'].copy()
    vol.loc[vol == 0.] = np.nan
    df['log_vol'] = np.log(vol)

    # Adds static information
    symbol_region_mapping = {
        '.AEX': 'EMEA',
        '.AORD': 'APAC',
        '.BFX': 'EMEA',
        '.BSESN': 'APAC',
        '.BVLG': 'EMEA',
        '.BVSP': 'AMER',
        '.DJI': 'AMER',
        '.FCHI': 'EMEA',
        '.FTMIB': 'EMEA',
        '.FTSE': 'EMEA',
        '.GDAXI': 'EMEA',
        '.GSPTSE': 'AMER',
        '.HSI': 'APAC',
        '.IBEX': 'EMEA',
        '.IXIC': 'AMER',
        '.KS11': 'APAC',
        '.KSE': 'APAC',
        '.MXX': 'AMER',
        '.N225': 'APAC ',
        '.NSEI': 'APAC',
        '.OMXC20': 'EMEA',
        '.OMXHPI': 'EMEA',
        '.OMXSPI': 'EMEA',
        '.OSEAX': 'EMEA',
        '.RUT': 'EMEA',
        '.SMSI': 'EMEA',
        '.SPX': 'AMER',
        '.SSEC': 'APAC',
        '.SSMI': 'EMEA',
        '.STI': 'APAC',
        '.STOXX50E': 'EMEA'
    }

    df['Region'] = df['Symbol'].apply(lambda k: symbol_region_mapping[k])

    # Performs final processing
    output_df_list = []
    for grp in df.groupby('Symbol'):
        sliced = grp[1].copy()
        sliced.sort_values('days_from_start', inplace=True)
        # Impute log volatility values
        sliced['log_vol'].fillna(method='ffill', inplace=True)
        sliced.dropna()
        output_df_list.append(sliced)

    df = pd.concat(output_df_list, axis=0)

    output_file = config.data_csv_path
    print('Completed formatting, saving to {}'.format(output_file))
    df.to_csv(output_file)

    print('Done.')


class Config:
    def __init__(self, data_folder, csv_path, source_csv_path):
        self.data_folder = data_folder
        self.data_csv_path = csv_path
        self.source_data_csv_path = source_csv_path


if __name__ == '__main__':
    config = Config('data', 'data/volatility.csv', 'oxfordmanrealizedvolatilityindices.csv')
    process_volatility_file(config)
