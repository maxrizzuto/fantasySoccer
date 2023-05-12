import pymysql
import csv
import pandas as pd


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


def create_database(conn):

    # get data from csv
    df = pd.read_csv('data/temp_data.csv')
    df = df.apply(pd.to_numeric, errors='ignore')

    # delete and recreate database
    with conn.cursor() as cursor:
        cursor.execute('DROP DATABASE IF EXISTS fantasy;')
        cursor.execute('CREATE DATABASE fantasy;')
        cursor.execute('USE fantasy;')

    # get zipped list of column names and dtypes
    dtypes = [str(x) for x in df.dtypes.tolist()]
    subs = {'int64': 'INT', 'float64': 'FLOAT', 'object': 'VARCHAR(255)'}
    subs_dct = {k: v for k, v in subs.items()}
    dtypes = [subs_dct.get(item, item) for item in dtypes]
    sql_cols = zip(df.columns, dtypes)

    # create player_game table if it doesn't exist
    with conn.cursor() as cursor:
        player_game = 'CREATE TABLE IF NOT EXISTS player_game ({}'.format(
            ', '.join([' '.join(x) for x in sql_cols])) + ', PRIMARY KEY (playerID, gw));'
        cursor.execute(player_game)

    # insert data from csv into sql
    cols = df.columns
    with conn.cursor() as cursor:
        f = csv.reader(open('data/temp_data.csv'))
        sql = f'INSERT INTO player_game ({", ".join(cols)}) VALUES ({", ".join(["%s"] * len(cols))})'
        next(f)
        for line in f:
            line = [None if cell == '' else cell for cell in line]
            cursor.execute(sql, line)
        conn.commit()
    print('-----------------\n', "Imported into player_game\n", '-----------------')


create_database(mysqlconnect())
