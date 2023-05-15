import pymysql
import csv
import pandas as pd
from scraping import get_match_data, get_player_data

PL_URL = 'https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures'


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


def create_database(conn, gw, flush=False):

    # get data from csv
    match_df = get_match_data(PL_URL, gw=gw)

    # delete and recreate database
    if flush:
        with conn.cursor() as cursor:
            cursor.execute('DROP DATABASE IF EXISTS fantasy;')
            cursor.execute('CREATE DATABASE fantasy;')
            cursor.execute('USE fantasy;')
        conn.commit()

    # get zipped list of column names and dtypes
    dtypes = [str(x) for x in match_df.dtypes.tolist()]
    subs = {'int64': 'INT', 'float64': 'FLOAT', 'object': 'VARCHAR(255)'}
    subs_dct = {k: v for k, v in subs.items()}
    dtypes = [subs_dct.get(item, item) for item in dtypes]
    sql_cols = zip(match_df.columns, dtypes)

    # create player_game table if it doesn't exist
    with conn.cursor() as cursor:
        player_game = 'CREATE TABLE IF NOT EXISTS player_game ({}'.format(
            ', '.join([' '.join(x) for x in sql_cols])) + ', PRIMARY KEY (playerID, gw, Club));'
        cursor.execute(player_game)
    conn.commit()

    # insert data from csv into sql
    cols = match_df.columns
    with conn.cursor() as cursor:

        # insert data from dataframe into sql
        sql = f'INSERT INTO player_game ({", ".join(cols)}) VALUES ({", ".join(["%s"] * len(cols))})'
        for row in match_df.itertuples(index=False):
            # convert row to list of values
            values = tuple(row)
            try:
                cursor.execute(sql, values)
            except pymysql.err.IntegrityError:
                print(
                    f'Duplicate entry found: {values[1]} ({values[2]})\nSkipping...\n')
                continue
        conn.commit()

    print('-----------------\n', "Imported into player_game\n", '-----------------')

    # create player table if it doesn't exist
    # with conn.cursor() as cursor:


if __name__ == '__main__':
    create_database(mysqlconnect(), 7)
