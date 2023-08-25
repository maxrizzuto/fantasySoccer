import pymysql
import requests
import time
from bs4 import BeautifulSoup as bs
import pandas as pd
from scraping import get_match_data, get_player_data

HOME_URL = 'https://fbref.com/'
PL_URLS = {
    'matches': 'https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures',
    'players': 'https://fbref.com/en/comps/9/Premier-League-Stats'}
LALIGA_URLS = {
    'matches': 'https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures',
    'players': 'https://fbref.com/en/comps/12/La-Liga-Stats'}
SERIEA_URLS = {
    'matches': 'https://fbref.com/en/comps/11/schedule/Serie-A-Scores-and-Fixtures',
    'players': 'https://fbref.com/en/comps/11/Serie-A-Stats'}
BUNDESLIGA_URLS = {
    'matches': 'https://fbref.com/en/comps/20/schedule/Bundesliga-Scores-and-Fixtures',
    'players': 'https://fbref.com/en/comps/20/Bundesliga-Stats'}
LIGUE1_URLS = {
    'matches': 'https://fbref.com/en/comps/13/schedule/Ligue-1-Scores-and-Fixtures',
    'players': 'https://fbref.com/en/comps/13/Ligue-1-Stats'}
URLS = [PL_URLS, LALIGA_URLS, SERIEA_URLS, BUNDESLIGA_URLS, LIGUE1_URLS]


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
            ' DEFAULT 0, '.join([' '.join(x) for x in match_cols])) + ', PRIMARY KEY (playerID, gw), CONSTRAINT player_game_fk FOREIGN KEY (playerID) REFERENCES player(playerID));'
        cursor.execute(player_game)
    conn.commit()

    # insert data from csv into sql
    cols = match_df.columns

    # create player_stats table if it doesn't exist
    create_player_stats_table(conn, cols)

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

    print('Imported into player_game\n-----------------')


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

                # wait for 4 seconds to avoid being blocked
                time.sleep(4)

                continue
        conn.commit()

    print('Imported into player\n-----------------\n\n')


def create_player_stats_table(conn, cols):

    # create table with all the same columns as player_game table, but with playerID as foreign key from player table and without the gameweek and player columns
    with conn.cursor() as cursor:
        # create table with all the same columns as player_game table, but with playerID as foreign key from player table and without the columns gw, Player, Nation, Club, Num, Pos, Age
        # Drop table if it exists
        cursor.execute(
            'CREATE TABLE IF NOT EXISTS player_stats LIKE player_game;')

        # Check if columns exist
        cursor.execute("""
            SELECT COUNT(*) INTO @column_count
            FROM information_schema.columns
            WHERE table_schema = 'fantasy'
            AND table_name = 'player_stats'
            AND column_name IN ('Player', 'gw', 'Nation', 'Club', 'Num', 'Pos', 'Age');
        """)

        # Drop columns if they exist
        cursor.execute("""
            SET @drop_statement = IF(@column_count > 0,
                'ALTER TABLE player_stats
                DROP COLUMN Player,
                DROP COLUMN gw,
                DROP COLUMN Nation,
                DROP COLUMN Club,
                DROP COLUMN Num,
                DROP COLUMN Pos,
                DROP COLUMN Age;',
                'SELECT "Columns do not exist.";'
            );
        """)

        cursor.execute("PREPARE stmt FROM @drop_statement;")
        cursor.execute("EXECUTE stmt;")
        cursor.execute("DEALLOCATE PREPARE stmt;")

        # check if foreign key exists, add it if it doesn't
        cursor.execute("""
            SELECT COUNT(*) INTO @fk_count
            FROM information_schema.TABLE_CONSTRAINTS
            WHERE CONSTRAINT_SCHEMA = 'fantasy'
            AND CONSTRAINT_NAME = 'player_stats_fk';
        """)
        cursor.execute("""
            SET @fk_statement = IF(@fk_count = 0,
                'ALTER TABLE player_stats
                ADD CONSTRAINT player_stats_fk FOREIGN KEY (playerID) REFERENCES player(playerID);',
                'SELECT "Foreign key already exists.";'
            );
        """)

        # Define the column names for summing and averaging
        non_cols = ['Player', 'gw', 'Nation',
                    'Club', 'Num', 'Pos', 'Age', 'playerID']
        sum_cols = [col for col in cols if col not in non_cols and 'pct' not in col.lower(
        ) and 'avg' not in col.lower()]
        avg_cols = [col for col in cols if col not in non_cols and (
            'pct' in col.lower() or 'avg' in col.lower())]

        # procedure for checking if players are in player_stats, adding if not
        player_procedure = """
        CREATE PROCEDURE IF NOT EXISTS insert_player_stats (playerIDVar varchar(255))
        BEGIN
            -- Check if playerID exists in player_stats table
            IF NOT EXISTS (SELECT 1 FROM player_stats WHERE playerID = playerIDVar) THEN
                -- playerID does not exist, insert it into player_stats table
                INSERT INTO player_stats (playerID) VALUES (playerIDVar);
            END IF;
        END;
        """
        cursor.execute(player_procedure)

        # Define the trigger codes with placeholders for the column names
        ins_trigger_code = """
        CREATE TRIGGER IF NOT EXISTS update_player_stats_onIns AFTER INSERT ON player_game
        FOR EACH ROW
        BEGIN
            CALL insert_player_stats(NEW.playerID);

            UPDATE player_stats
            SET {sum_cols}
            WHERE playerID = NEW.playerID;

            UPDATE player_stats
            SET {avg_cols}
            WHERE playerID = NEW.playerID;
        END;
        """

        del_trigger_code = ins_trigger_code.replace(
            'INSERT', 'DELETE').replace('onIns', 'onDel').replace('NEW', 'OLD')
        upd_trigger_code = ins_trigger_code.replace(
            'INSERT', 'UPDATE').replace('onIns', 'onUpd')
        triggers = [ins_trigger_code, del_trigger_code, upd_trigger_code]

        # Format the column names for summing
        sum_cols_str = ', '.join(
            f"{col} = {col} + NEW.{col}" for col in sum_cols)

        # Format the column names for averaging
        avg_cols_str = ', '.join(f"{col} = (\
                SELECT AVG({col}) FROM player_game WHERE playerID = NEW.playerID)" for col in avg_cols)

        # Format the trigger codes with the column names
        for trigger in triggers:
            trigger_code = trigger.split(' AFTER ')[0].replace(
                'CREATE TRIGGER', 'DROP TRIGGER IF EXISTS') + ';'
            if 'AFTER DELETE ON' in trigger:
                trigger_code = trigger.format(
                    sum_cols=sum_cols_str.replace('NEW', 'OLD'), avg_cols=avg_cols_str.replace('NEW', 'OLD'))
            else:
                trigger_code = trigger.format(
                    sum_cols=sum_cols_str, avg_cols=avg_cols_str)
            cursor.execute(trigger_code)
    conn.commit()


def create_database(conn, gws, flush=False, players=False):

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

    if players:
        create_player_table(conn)

    for gw in gws:
        create_player_game_table(conn, gw)


if __name__ == '__main__':
    create_database(mysqlconnect(), [1, 2], flush=True)
