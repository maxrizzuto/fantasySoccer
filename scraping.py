import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup as bs
import time

HOME_URL = 'https://fbref.com/'
PL_URLS = {'matches': 'https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures',
           'players': 'https://fbref.com/en/comps/9/Premier-League-Stats'}


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

    # if second value in player tuple is none remove the row from the dataframe
    df = df[df['Player'].map(lambda x: x[1] is not None)]

    # extract player id from player column
    if isinstance(df['Player'].iloc[0], tuple):
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
        # drop rows with empty nation
        df = df.dropna(subset=['Nation'])
        df['Nation'] = df['Nation'].map(lambda x: x.split(' ')[-1])

    # take first position from Pos column
    if 'Pos' in df.columns:
        # drop rows with empty position
        df = df.dropna(subset=['Pos'])
        df['Pos'] = df['Pos'].map(lambda x: x.split(',')[0])

    # take first value from Age column
    if 'Age' in df.columns:
        # drop rows with empty age
        df = df.dropna(subset=['Age'])
        df['Age'] = df['Age'].map(lambda x: x.split('-')[0])

    return df


def get_match_data(url, gw, export=False):

    # get dataframe from url
    df = get_dataframe(url, gw=gw)

    # get link from each match in dataframe
    matches = [f'https://fbref.com{x}' for x in df.Score.map(lambda x: x[1])]

    # create empty dataframe to store match data
    gw_df = pd.DataFrame(columns=['Player', 'playerID'])

    # loop through each match and get match data
    for match in matches:

        # extract data, create empty dataframe for storage
        df = pd.read_html(match, extract_links='all')[3:]
        match_df = pd.DataFrame(columns=['Player', 'playerID', 'Club'])

        # get team names
        data = requests.get(match).text
        soup = bs(data, 'html.parser')

        # get team names and assign to corresponding dfs
        df_lst = list()
        for x in df:
            # remove first part of multi-index
            cols = [x[-1][0] for x in x.columns]
            if 'Event' not in cols:
                df_lst.append(x)

        # add teams to df
        teams = [x.find('a').text for x in soup.find_all('strong') if x.find(
            'a') and '/en/squads/' in x.find('a')['href']]
        print(f'GW {gw}: Retrieving data for {teams[0]} vs. {teams[1]}')
        team_a = df_lst[:len(df_lst)//2]
        team_b = df_lst[len(df_lst)//2:]
        for x in team_a:
            x['Club'] = teams[0]
        for x in team_b:
            x['Club'] = teams[1]
        df = team_a + team_b

        for x in df:
            # remove first part of multi-index
            x.columns = [x[-1][0] if x != 'Club' else x for x in x.columns]

            # remove null values from tuples
            x = x.applymap(lambda x: x if x[1] else x[0])

            # make playerID column
            x = clean_columns(x)

            # convert to numeric values
            x = x.apply(pd.to_numeric, errors='ignore')

            # merge with match_df on player and player_id columns
            if 'Event' in x.columns:
                continue
            match_df = pd.merge(
                match_df, x, how='outer')

            # group by player and player_id, take non null value
            match_df = match_df.groupby(
                ['Player', 'playerID', 'Club']).first().reset_index()

        # add match_df to gw_df
        gw_df = pd.concat([gw_df, match_df], ignore_index=True)
        print('Done.\n--------------------------')

        # sleep for 5 seconds to avoid getting blocked
        time.sleep(5)

    # convert to numeric values
    gw_df = gw_df.apply(pd.to_numeric, errors='ignore')

    # move gw column to first column
    gw_df['gw'] = gw
    cols = gw_df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    gw_df = gw_df[cols]

    # rename columns to be sql-friendly
    cols = gw_df.columns.tolist()
    cols = [sub.replace('#', 'Num').replace('Int', 'Interceptions').replace(
        '%', 'Pct').replace('1/3', 'Passes_Final_Third').replace('2', 'Second').replace('Out', 'Outswinging').replace('Off', 'Pass_Offside').replace('+', 'And')
        .replace(' ', '_') if sub != 'In' else 'Inswinging' for sub in cols]
    gw_df.columns = cols
    gw_df = gw_df.fillna(0)

    # export if specified
    if export:
        gw_df.to_csv(f'/data/match_data/gw{gw}.csv', index=False)

    return gw_df


def get_player_data(url):
    """Scrape permanent data for each player's profile"""

    # get url for each team
    df = get_dataframe(url)
    team_urls = df['Squad'].map(lambda x: HOME_URL + x[1]).values

    # create empty dataframe to store player data
    player_df = pd.DataFrame()

    # get data for each team
    for team in team_urls:
        team_name = ' '.join(team.split('/')[-1].split('-')[:-1])
        print(f'Retrieving data for {team_name}...')
        df = pd.read_html(team, extract_links='all')[0]
        df.columns = [x[-1][0] for x in df.columns]
        df = df.T.groupby(level=0).first().T
        df['Club'] = team_name

        # get player_id column
        df = clean_columns(df)

        # move player, nation, pos, age, mp, starts, and mins to first columns
        cols = ['Player', 'playerID', 'Club', 'Nation',
                'Pos', 'Age', 'MP', 'Starts', 'Min']
        cols = cols[::-1]

        # clean columns
        df = clean_columns(df)
        df = df.drop(columns='Matches')
        df = df.fillna(0)

        # move each col to front of df, remove unnecessary columns
        for col in cols:
            df_cols = df.columns.tolist()
            df_cols.insert(0, df_cols.pop(df_cols.index(col)))
        df = df[cols[::-1]]

        # convert to numeric columns where possible
        df = df.apply(pd.to_numeric, errors='ignore')

        # confirm data was retrieved
        print('Done.\n--------------------------')

        # add df to player_df
        player_df = pd.concat([player_df, df], ignore_index=True)

        # sleep for 3 seconds to avoid getting blocked
        time.sleep(3)

    # change all columns in dataframe to numeric where possible
    player_df = player_df.apply(pd.to_numeric, errors='ignore')

    return player_df


if __name__ == '__main__':
    get_player_data(PL_URLS['players'])
    # get_match_data(PL_URLS['matches'], 1)
