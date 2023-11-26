import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup as bs
import time
from urllib.request import urlopen, Request
from lxml import etree

HOME_URL = 'https://fbref.com/'
URLS = {
    'Premier League': {
        'matches': 'https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures',
        'players': 'https://fbref.com/en/comps/9/Premier-League-Stats'
    },
    'La Liga': {
        'matches': 'https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures',
        'players': 'https://fbref.com/en/comps/12/La-Liga-Stats'
    },
    'Serie A': {
        'matches': 'https://fbref.com/en/comps/11/schedule/Serie-A-Scores-and-Fixtures',
        'players': 'https://fbref.com/en/comps/11/Serie-A-Stats'
    },
    'Bundesliga': {
        'matches': 'https://fbref.com/en/comps/20/schedule/Bundesliga-Scores-and-Fixtures',
        'players': 'https://fbref.com/en/comps/20/Bundesliga-Stats'}
}


def get_dataframe(url, gw=None):
    time.sleep(3)
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

def get_player_pos(url):
    """Scrape player position from their profile page"""

    # get url for each player and scrape their page to get actual position
    time.sleep(3)
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    response = urlopen(req)
    htmlparser = etree.HTMLParser()
    tree = etree.parse(response, htmlparser)

    # get their specific position if they have a scout report
    try:
        pos = tree.xpath('//div[@id="all_similar"]/div[@class="filter switcher"]/div/a')[0].text[:-1]
    except IndexError:
        # if they don't have a scout report, get their general position
        pos = tree.xpath('//div[@id="meta"]//p/strong[text()="Position:"]/../text()')[0].strip()
        if '(' in pos:
            pos = pos.split('(')[-1].split(')')[0]
            if '-' in pos:
                pos = pos.split('-')[0]
        else:
            pos = pos[:2]
    return pos

def clean_columns(df, pos=False):

    # drop duplicate player column if it exists, remove empty players
    df = df.loc[:, ~df.columns.duplicated()]
    df = df.replace(r'^\s*$', np.nan, regex=True)
    df = df.dropna(subset=['Player'])

    # remove last row if it's a sum of all other rows
    if df['Player'].str.contains('Players').any():
        df = df[:-1]

    # if second value in player tuple is none remove the row from the dataframe
    df = df[df['Player'].map(lambda x: x[1] is not None)]

    # make player_url a column
    df['player_url'] = df['Player'].map(lambda x: HOME_URL[:-1] + x[1])

    # update position and drop empties
    if pos:
        # iterate through each player_url and get their position
        for url in df['player_url']:
            pos = get_player_pos(url)
            df.loc[df['player_url'] == url, 'Pos'] = pos

        # drop rows with empty position
        df = df.dropna(subset=['Pos'])
        df['Pos'] = df['Pos'].map(lambda x: x.split(',')[0])

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

    # take first value from Age column
    if 'Age' in df.columns:
        # drop rows with empty age
        df = df.dropna(subset=['Age'])
        df['Age'] = df['Age'].map(lambda x: x.split('-')[0])

    # fill all na or empty values with 0
    df = df.replace(r'^\s*$', np.nan, regex=True)
    df = df.fillna(0)

    return df

def get_matches(url, gw, export=False):
    df = get_dataframe(url, gw=gw)
    
    # drop postponed matches or matches with no score listed
    df.Score.replace('', np.nan, inplace=True)
    df.dropna(subset=['Score'], inplace=True)

    # get match id
    df['ID'] = df['Score'].map(lambda x: x[1].split('/')[2])

    # get second value from score column and first value from other tuples
    df = df.applymap(lambda x: x[0] if isinstance(x, tuple) else x)

    # get home and away score
    df['home_score'] = df['Score'].map(lambda x: x.split('–')[0])
    df['away_score'] = df['Score'].map(lambda x: x.split('–')[1])

    # rename Wk to gw and change cols to lowercase
    df.rename(columns={'Wk': 'gw'}, inplace=True)
    df.columns = [col.lower() for col in df.columns]
    df = df[['id', 'gw', 'home', 'away', 'home_score', 'away_score']]

    # export if specified
    if export:
        league = url.split('/')[-1].replace('-Scores-and-Fixtures', '')
        df.to_csv(f'data/match_data/{league}_gw{gw}.csv', index=False)

    return df


def get_player_match_data(league, gw, export=False):
    # get dataframe from url
    url = URLS[league]['matches']
    df = get_dataframe(url, gw=gw)

    # drop postponed matches or matches with no score listed
    df.Score.replace('', np.nan, inplace=True)
    df.dropna(subset=['Score'], inplace=True)

    # get link from each match in dataframe
    matches = [f'https://fbref.com{x}' for x in df.Score.map(lambda x: x[1])]

    # create empty dataframe to store match data
    gw_df = pd.DataFrame(columns=['Player', 'playerID'])

    # loop through each match and get match data
    for match in matches:

        # extract data, create empty dataframe for storage
        time.sleep(3)
        df = pd.read_html(match, extract_links='all')[3:]
        # create empty dataframe to store match data with playerID column dtype set to string
        match_df = pd.DataFrame(columns=['Player', 'playerID', 'Club']).astype({'playerID': str})

        # get team names
        data = requests.get(match).text
        soup = bs(data, 'html.parser')

        # get team names and assign to corresponding dfs
        df_lst = [x for x in df if 'Event' not in [col[-1][0] for col in x.columns]]

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
        df_lst = team_a + team_b

        for df in df_lst:
            # Remove first part of multi-index
            df.columns = [col[-1][0] if col != 'Club' else col for col in df.columns]

            # Remove null values from tuples
            df = df.applymap(lambda val: val if val[1] else val[0])

            # Make playerID column
            df = clean_columns(df)

            # Convert to numeric values
            df = df.apply(pd.to_numeric, errors='ignore')

            # Check for 'Event' column before merging
            if 'Event' not in df.columns:

                # set playerID column in df to string type
                df['playerID'] = df['playerID'].astype(str)

                # Merge with match_df on player and player_id columns
                match_df = pd.merge(match_df, df, how='outer')

        # Group by player and player_id, take non-null value
        match_df = match_df.groupby(['Player', 'playerID', 'Club']).first().reset_index()

        # drop position column since accurate version is in player data
        match_df = match_df.drop(columns='Pos')
        
        # get match id 
        match_id = match.split('/')[-2]
        match_df['matchID'] = match_id

        # add match_df to gw_df
        gw_df = pd.concat([gw_df, match_df], ignore_index=True)
        print('Done.\n--------------------------')

    # convert to numeric values
    gw_df = gw_df.apply(pd.to_numeric, errors='ignore')

    # move gw column to first column
    gw_df['gw'] = gw
    cols = gw_df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    gw_df = gw_df[cols]

    # rename columns to be sql-friendly
    cols = gw_df.columns.tolist()
    gw_df.columns = [sub.replace('Int', 'Interceptions').replace(
        '%', 'Pct').replace('1/3', 'Passes_Final_Third').replace('2', 'Second').replace('Out', 'Outswinging').replace('Off', 'Pass_Offside').replace('+', 'And')
        .replace(' ', '_') if sub != 'In' else 'Inswinging' for sub in cols]
    gw_df = gw_df.fillna(0)

    # drop num column, add league and pos columns
    gw_df['League'] = league
    gw_df['Pos'] = 'N/A'

    # drop all these columns
    cols = ['player_url', 'gw', 'Player', 'Pos', 
            'Nation', 'Age']
    gw_df = gw_df.drop(columns=cols)

    # move matchid, playerid, and min to the front
    cols = ['matchID', 'playerID', 'League', 'Club', 'Min']
    cols = cols[::-1]
    df_cols = gw_df.columns.tolist()
    for col in cols:
        df_cols.insert(0, df_cols.pop(df_cols.index(col)))
    gw_df = gw_df[df_cols]

    # convert to firestore columns
    gw_df.columns = [col.lower() for col in gw_df.columns]
    gw_df.rename(columns={'pos': 'position', 'player': 'name', 'mp': 'matches', 'min': 'mins'}, inplace=True)

    # export if specified
    if export:
        league = url.split('/')[-1].replace('-Scores-and-Fixtures', '')
        gw_df.to_csv(f'data/match_data/{league}_gw{gw}.csv', index=False)

    return gw_df

def get_player_data(league, export=False):
    """Scrape permanent data for each player's profile"""
    # get url for each team
    url = URLS[league]['players']
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
        df['League'] = league

        # move player, nation, pos, age, mp, starts, and mins to first columns
        cols = ['playerID', 'player_url', 'Player', 'Pos', 'Club', 'League', 
                'Nation', 'Age', 'MP', 'Starts', 'Min']
        cols = cols[::-1]

        # clean columns
        df = clean_columns(df, pos=True)
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

    # modify columns for firestore
    player_df.columns = [col.lower() for col in player_df.columns]
    player_df.rename(columns={'pos': 'position', 'player': 'name', 'mp': 'matches', 'min': 'mins', 'playerid': 'ID'}, inplace=True)


    if export:
        league = url.split('/')[-1].replace('-Stats', '')
        player_df.to_csv(f'data/player_data/{league}_player_data.csv', index=False)

    return player_df


if __name__ == '__main__':
    for league in URLS.keys():
        get_player_data(URLS[league]['players'], export=True, league=league)
        for gw in range(1, 2):
            get_player_match_data(URLS[league]['matches'], gw, export=True, league=league)
