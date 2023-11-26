import pymysql
import requests
import time
from bs4 import BeautifulSoup as bs
from scraping import get_match_data, get_player_data

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
    subs = {'int64': 'INT DEFAULT 0', 'float64': 'FLOAT DEFAULT 0', 'object': 'VARCHAR(255)'}
    subs_dct = {k: v for k, v in subs.items()}
    dtypes = [subs_dct.get(item, item) for item in dtypes]
    sql_cols = zip(df.columns, dtypes)
    return sql_cols


def add_pos_triggers(conn):
    # create trigger that updates Pos in player_game and player_stats on insert to player table
    with conn.cursor() as cursor:
        trigger_code = """
        CREATE TRIGGER IF NOT EXISTS update_pos_onIns AFTER INSERT ON player
        FOR EACH ROW
        BEGIN
            UPDATE player_game
            SET Pos = NEW.Pos
            WHERE playerID = NEW.playerID;

            UPDATE player_stats
            SET Pos = NEW.Pos
            WHERE playerID = NEW.playerID;
        END;
        """
        cursor.execute(trigger_code)

        # create the same trigger for updates
        trigger_code = """
        CREATE TRIGGER IF NOT EXISTS update_pos_onUpd AFTER UPDATE ON player
        FOR EACH ROW
        BEGIN
            UPDATE player_game
            SET Pos = NEW.Pos
            WHERE playerID = NEW.playerID;

            UPDATE player_stats
            SET Pos = NEW.Pos
            WHERE playerID = NEW.playerID;
        END;
        """
        cursor.execute(trigger_code)
    conn.commit()


def update_pos(conn):
    # write a procedure that updates all Pos in player_game and player_stats
    with conn.cursor() as cursor:
        procedure_code = """
        CREATE PROCEDURE IF NOT EXISTS update_pos()
        BEGIN
            UPDATE player_game
            SET Pos = (SELECT Pos FROM player WHERE playerID = player_game.playerID);

            UPDATE player_stats
            SET Pos = (SELECT Pos FROM player WHERE playerID = player_stats.playerID);
        END;
        """
        cursor.execute(procedure_code)
        cursor.execute('CALL update_pos();')
    conn.commit()


def create_player_game_table(conn, gw_map):

    # iterate through each league and according gameweek
    for league, gws in gw_map.items():

        # check if gws is a string
        if type(gws) == str: 
            gws = [gws]

        # get list of gameweeks based on input
        elif len(gws) == 1:
            gws = list(range(1, gws[0] + 1))
        for gw in gws:
            match_df = get_match_data(URLS[league]['matches'], gw=gw, league=league)
            match_df['League'] = league

            # create player_game table if it doesn't exist
            match_cols = sql_cols(match_df)
            col_insert = ', '.join([' '.join(x) for x in match_cols])
            counter = 0
            with conn.cursor() as cursor:
                counter += 1
                player_game = f'CREATE TABLE IF NOT EXISTS player_game ({col_insert} , PRIMARY KEY (playerID, gw), CONSTRAINT player_game_fk FOREIGN KEY (playerID) REFERENCES player(playerID));'
                cursor.execute(player_game)

            # add pos triggers
            add_pos_triggers(conn)
                
            # create player_stats table if it doesn't exist and commit
            cols = match_df.columns
            create_player_stats_table(conn, cols)
            conn.commit()

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
            print(f'Imported into player_game ({league}, GW {gw})\n-----------------')


def create_player_table(conn):
    # create player table if it doesn't exist
    for league in URLS.keys():
        if league != 'Bundesliga':
            continue
        player_df = get_player_data(URLS[league]['players'], league=league)
        player_df['League'] = league
        player_cols = sql_cols(player_df)
        with conn.cursor() as cursor:
            player = 'CREATE TABLE IF NOT EXISTS player ({}'.format(
                ', '.join([' '.join(x) for x in player_cols])) + ', PRIMARY KEY (playerID));'
            cursor.execute(player)
        conn.commit()

        #  print message confirming table is created
        print("Created player table\n--------------------------")

        # change player_df column order
        player_df = player_df[['playerID', 'player_url', 'Player', 'Pos', 'Club', 'League', 'Nation', 'Num', 'Age', 'MP', 'Starts', 'Min']]

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
        print(f'Imported into player ({league})\n-----------------\n\n')


def create_player_stats_table(conn, cols):

    # create table with all the same columns as player_game table, but with playerID as foreign key from player table and without the gameweek and player columns
    with conn.cursor() as cursor:
        # create table with all the same columns as player_game table, but with playerID as foreign key from player table and without the columns gw, Player, Nation, Club, Num, Pos, Age
        # Drop table if it exists
        cursor.execute(
            'CREATE TABLE IF NOT EXISTS player_stats LIKE player_game;')
        
        # add pos column with varchar(255) type if it doesn't exist
        cursor.execute("""
            SELECT COUNT(*) INTO @pos_count
            FROM information_schema.columns
            WHERE table_schema = 'fantasy'
            AND table_name = 'player_stats'
            AND column_name = 'Pos';
        """)
        cursor.execute("""
            SET @pos_statement = IF(@pos_count = 0,
                'ALTER TABLE player_stats
                ADD COLUMN Pos VARCHAR(255) AFTER player_url;',
                'SELECT "Column already exists."'
            );
        """)

        # check if gw is in cols, drop if it is
        cursor.execute("""
            SELECT COUNT(*) INTO @gw_count
            FROM information_schema.columns
            WHERE table_schema = 'fantasy'
            AND table_name = 'player_stats'
            AND column_name = 'gw';
        """)
        cursor.execute("""
            SET @gw_statement = IF(@gw_count = 1,
                'ALTER TABLE player_stats
                DROP COLUMN gw;',
                'SELECT "Column does not exist."'
            );
        """)
        cursor.execute("PREPARE stmt FROM @gw_statement;")
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
        cursor.execute("PREPARE stmt FROM @fk_statement;")
        cursor.execute("EXECUTE stmt;")
        cursor.execute("DEALLOCATE PREPARE stmt;")
        conn.commit()

        # Define the column names for summing and averaging
        non_cols = ['Player', 'Pos', 'gw', 'Nation',
                    'Club', 'League', 'Num', 'Age', 'playerID', 'player_url']
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
                SELECT Player, player_url, Pos, Club, League, Age, Nation INTO @playerNameVar, @playerUrlVar, @playerPosVar, @PlayerClubVar, @PlayerLeagueVar, @PlayerAgeVar, @PlayerNationVar
                FROM player
                WHERE playerID = playerIDVar;
                
                -- Insert playerID, playerName, and playerURL into player_stats table
                INSERT INTO player_stats (playerID, Player, player_url, Pos, Club, League, Age, Nation)
                VALUES (playerIDVar, @playerNameVar, @playerUrlVar, @playerPosVar, @PlayerClubVar, @PlayerLeagueVar, @PlayerAgeVar, @PlayerNationVar);
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

        # other triggers
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
            trigger_code = trigger_code.strip()
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

        # check if the user is sure
        confirmation = input('Are you sure? This cannot be undone. Type YES to continue...\n>>> ')

        if confirmation != 'YES':
            print('Exiting...')
            return

        with conn.cursor() as cursor:
            cursor.execute('DROP DATABASE IF EXISTS fantasy;')
            cursor.execute('CREATE DATABASE fantasy;')
            cursor.execute('USE fantasy;')
        conn.commit()

    if players:
        create_player_table(conn)

    create_player_game_table(conn, gws)
    update_pos(conn)


if __name__ == '__main__':
    create_database(
        mysqlconnect(), 
        {
            'Premier League': [2],
            'La Liga': [2], 
            'Serie A': [1], 
            'Bundesliga': [1]
        },
        # flush=True,
        # players=True
    )
