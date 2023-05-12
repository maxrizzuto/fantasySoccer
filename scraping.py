import pandas as pd
import numpy as np


def get_dataframe(url, gw=None):
    df = pd.read_html(url, extract_links='all')[0]

    # remove null values from tuples
    df = df.applymap(lambda x: x if x[1] else x[0])

    # remove the second value of each tuple from the column names
    df.columns = df.columns.map(lambda x: x[0])

    # convert columns to numeric where possible
    df = df.apply(pd.to_numeric, errors='ignore')

    # select gw if specified
    if gw:
        df = df[df['Wk'] == gw]

    return df


def clean_columns(df):

    # drop duplicate player column if it exists, remove empty players
    df = df.loc[:, ~df.columns.duplicated()]
    df = df.replace(r'^\s*$', np.nan, regex=True)
    df = df.dropna(subset=['Player'])

    # remove last row if it's a sum of all other rows
    if df['Player'].str.contains('Players').any():
        df = df[:-1]

    # extract player id from player column
    df['playerID'] = df['Player'].map(lambda x: x[1].split('/')[-2])
    df['Player'] = df['Player'].map(lambda x: x[0])

    # move playerID to first column
    cols = df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    df = df[cols]

    # change all tuple columns to their first value
    df = df.applymap(lambda x: x[0] if isinstance(x, tuple) else x)

    # split Nation on space and take last value
    if 'Nation' in df.columns:
        df['Nation'] = df['Nation'].map(lambda x: x.split(' ')[-1])

    # take first position from Pos column
    if 'Pos' in df.columns:
        df['Pos'] = df['Pos'].map(lambda x: x.split(',')[0])

    # take first value from Age column
    if 'Age' in df.columns:
        df['Age'] = df['Age'].map(lambda x: x.split('-')[0])

    return df


def get_match_data(df):

    # get link from each match in dataframe
    matches = [f'https://fbref.com{x}' for x in df.Score.map(lambda x: x[1])]

    # create empty dataframe to store match data
    gw_df = pd.DataFrame(columns=['Player', 'playerID'])

    # loop through each match and get match data
    for match in matches:

        # extract data, create empty dataframe for storage
        df = pd.read_html(match, extract_links='all')[3:]
        match_df = pd.DataFrame(columns=['Player', 'playerID'])

        for x in df:
            # remove first part of multi-index
            x.columns = [x[-1][0] for x in x.columns]

            # remove null values from tuples
            x = x.applymap(lambda x: x if x[1] else x[0])

            # make playerID column
            x = clean_columns(x)

            # merge with match_df on player and player_id columns
            if 'Event' in x.columns:
                continue
            match_df = pd.merge(
                match_df, x, how='outer')

            # group by player and player_id, take non null value
            match_df = match_df.groupby(
                ['Player', 'playerID']).first().reset_index()

        # add match_df to gw_df
        gw_df = pd.concat([gw_df, match_df], ignore_index=True)

    return gw_df


def export_data(url, gw):

    df = get_match_data(get_dataframe(url, gw=gw))
    df['gw'] = gw

    # move gw column to first column
    cols = df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    df = df[cols]

    df.to_csv('data/temp_data.csv', index=False)


export_data(
    'https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures', 3)
