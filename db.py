import pymysql
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


def check_database(conn):

    # get data from csv
    df = pd.read_csv('data/temp_data.csv')
    df = df.apply(pd.to_numeric, errors='ignore')

    # get columns and corresponding dtypes, convert to sql dtypes
    cols = df.columns.tolist()
    cols = [sub.replace('#', 'Num').replace('Int', 'Interceptions').replace(
        '%', 'Pct').replace('1/3', 'Passes_Final_Third').replace('2', 'Second').replace('Out', 'Outswinging').replace('Off', 'Pass_Offside').replace('+', 'And')
        .replace(' ', '_') if sub != 'In' else 'Inswinging' for sub in cols]
    dtypes = [str(x) for x in df.dtypes.tolist()]
    subs = {'int64': 'INT', 'float64': 'FLOAT', 'object': 'VARCHAR(255)'}
    subs_dct = {k: v for k, v in subs.items()}
    dtypes = [subs_dct.get(item, item) for item in dtypes]
    cols = zip(cols, dtypes)

    # create player_game table if it doesn't exist
    with conn.cursor() as cursor:
        player_game = 'CREATE TABLE IF NOT EXISTS player_game ({}'.format(
            ', '.join([' '.join(x) for x in cols])) + ', PRIMARY KEY (playerID, gw));'
        print(player_game)
        cursor.execute(player_game)


check_database(mysqlconnect())
