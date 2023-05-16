import pymysql
import requests
import time
from bs4 import BeautifulSoup as bs
import pandas as pd
from scraping import get_match_data, get_player_data

HOME_URL = 'https://fbref.com/'
PL_URLS = {'matches': 'https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures',
           'players': 'https://fbref.com/en/comps/9/Premier-League-Stats'}


def mysqlconnect():

    # read in db connection info
    with open('sql.config', 'r') as f:
        host = f.readline().split(': ')[1].strip()
        user = f.readline().split(': ')[1].strip()
        password = f.readline().split(': ')[1].strip()
        db = f.readline().split(': ')[1].strip()

    # connect to database
    conn = pymysql.connect(
        host=host,
        user=user,
        password=password,
        db=db
    )

    return conn


def sql_cols(df):
    dtypes = [str(x) for x in df.dtypes.tolist()]
    subs = {'int64': 'INT', 'float64': 'FLOAT', 'object': 'VARCHAR(255)'}
    subs_dct = {k: v for k, v in subs.items()}
    dtypes = [subs_dct.get(item, item) for item in dtypes]
    sql_cols = zip(df.columns, dtypes)
    return sql_cols


def create_player_game_table(conn, gw):

    # get data from csv
    match_df = get_match_data(PL_URLS['matches'], gw=gw)

    # create player_game table if it doesn't exist
    match_cols = sql_cols(match_df)
    with conn.cursor() as cursor:
        player_game = 'CREATE TABLE IF NOT EXISTS player_game ({}'.format(
            ', '.join([' '.join(x) for x in match_cols])) + ', PRIMARY KEY (playerID), CONSTRAINT player_fk FOREIGN KEY (playerID) REFERENCES player(playerID));'
        # PRIMARY KEY (playerID, gw, Club),
        cursor.execute(player_game)
    conn.commit()

    # insert data from csv into sql
    cols = match_df.columns
    with conn.cursor() as cursor:

        # insert data from dataframe into sql
        sql = f'INSERT INTO player_game ({", ".join(cols)}) VALUES ({", ".join(["%s"] * len(cols))})'
        for row in match_df.itertuples(index=False):
            values = tuple(row)
            try:
                cursor.execute(sql, values)
            except pymysql.err.IntegrityError:
                print(
                    f'Duplicate entry found: {values[1]} ({values[2]})\nSkipping...\n')
                continue
        conn.commit()

    print("Imported into player_game\n", '-----------------')


def create_player_table(conn):
    # create player table if it doesn't exist
    player_df = get_player_data(PL_URLS['players'])
    player_cols = sql_cols(player_df)
    with conn.cursor() as cursor:
        player = 'CREATE TABLE IF NOT EXISTS player ({}'.format(
            ', '.join([' '.join(x) for x in player_cols])) + ', PRIMARY KEY (playerID));'
        cursor.execute(player)
    conn.commit()

    #  print message confirming table is created
    print("Created player table\n--------------------------")

    # insert data from dataframe into sql
    cols = player_df.columns
    with conn.cursor() as cursor:

        # insert data from dataframe into sql
        sql = f'INSERT INTO player ({", ".join(cols)}) VALUES ({", ".join(["%s"] * len(cols))})'
        for row in player_df.itertuples(index=False):
            values = tuple(row)
            try:
                cursor.execute(sql, values)
            except pymysql.err.IntegrityError:
                print(
                    f'Duplicate entry found: {values[0]} ({values[1]}), {values[2]}\nUpdating club...\n')

                # scrape player's individual page and find current club, update accordingly

                player_url = f'{HOME_URL}/en/players/{values[1]}/{"-".join(values[0].split(" "))}'

                # find player's current club
                player_page = requests.get(player_url).text
                player_soup = bs(player_page, 'html.parser')
                club = player_soup.find(
                    'strong', string=lambda x: x and x == 'Club:').parent.find('a').text.replace('&', 'and')

                update_sql = f'UPDATE player SET Club = %s WHERE playerID = %s'
                cursor.execute(update_sql, (club, values[1]))

                # wait for 3 seconds to avoid being blocked
                time.sleep(3)

                continue
        conn.commit()

    print('-----------------\nImported into player\n-----------------')


def create_database(conn, gw, flush=False):

    # delete and recreate database
    if flush:
        confirmation = input(
            'This will delete and recreate the database. Type CONFIRM to continue...\n>>> ')

        if confirmation != 'CONFIRM':
            print('Exiting...')
            return

        print()

        with conn.cursor() as cursor:
            cursor.execute('DROP DATABASE IF EXISTS fantasy;')
            cursor.execute('CREATE DATABASE fantasy;')
            cursor.execute('USE fantasy;')
        conn.commit()

    # create player_game table and player table
    create_player_table(conn)
    create_player_game_table(conn, gw)

    # create table for players' aggregated stats with foreign key of player_id from player table


if __name__ == '__main__':
    create_database(mysqlconnect(), 9, flush=True)


"""

# avg stats: _______
# sum stats: _______

create procedure to update player table by summing stats from player_game table

create procedure update_player_table(playerID varchar(255))
begin
    # 
    


    



"""
