import os
import time

import firebase_admin
from firebase_admin import firestore
from google.api_core.exceptions import RetryError
from google.api_core.retry import Retry

from scraping import get_matches, get_player_data, get_player_match_data, get_player_pos

retry = Retry(deadline=120.0)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/max/fantasySoccer/credentials.json"

# cred = credentials.Certificate("credentials.json")
firebase_admin.initialize_app()
db = firestore.client()

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

def api_call(api_function, *args, max_retries=3, retry_delay=5, **kwargs):
    for _ in range(max_retries + 1):
        try:
            # Call the API function with potential params
            result = api_function(*args, **kwargs)
            return result
        except RetryError as e:
            print(f"RetryError: {e}")
            print(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

    # If all retries fail, you might want to raise an exception or handle it accordingly
    raise Exception("API request failed after multiple retries")


def insert_players_data():
    for league in URLS:
        players_data = get_player_data(league)
        # add league if doesn't exist
        if not db.collection('leagues').document(league).get().exists:
            api_call(db.collection('leagues').document(league).set, {})

        # add each club that dosnt't exist
        clubs = players_data['club'].unique().tolist()
        for club in clubs:
            club_ref = db.collection('leagues').document(league).collection('clubs').document(club)
            if not api_call(club_ref.get).exists:
                api_call(club_ref.set, {})
        players_data.apply(lambda player: api_call(db.collection('leagues').document(league).collection('clubs').document(player['club']).collection('players').document(player['ID']).set, {key: value for key, value in player.items() if key not in ['ID', 'club', 'league']}), axis=1)
        print(f'{league} players data inserted')


def insert_matches_data(gw_map):
    for league in gw_map:
        for gw in gw_map[league]:
            matches_data  = get_matches(URLS[league]['matches'], gw)
            player_matches_data = get_player_match_data(league, gw)

            # add gw if doesn't exist    
            gw_ref = db.collection('leagues').document(league).collection('matches').document(str(gw))
            if not api_call(gw_ref.get).exists:
                api_call(gw_ref.set, {})

            # insert matches
            matches_data.apply(lambda match: api_call(gw_ref.add, {match['id']: {key: value for key, value in match.items() if key not in ['playerid', 'matchid', 'club', 'gw']}}), axis=1)
   
            # insert player matches
            player_matches_data.apply(
                lambda player: 
                    api_call(db.collection('leagues').document(league).collection('clubs').document(player['club']).collection('players').document(player['playerid']).collection('games').document(player['matchid']).set, {key: value for key, value in player.items() if key not in ['playerid', 'matchid' 'club', 'league']}), axis=1)
            print(f'{league} matches data inserted')


def update_player_pos():
    players = db.collection('players').get()
    for player in players:
        player_data = player.to_dict()
        player_pos = get_player_pos(player_data['player_url'])
        api_call(db.collection('leagues').document(player_data['league']).collection('clubs').document(player_data['club']).collection('players').document(player_data['id']).update, {'pos': player_pos})
        print(f'{player_data["name"]} position updated')

if __name__ == '__main__':
    insert_players_data()
    insert_matches_data({'Premier League': [x for x in range(1, 13)], 'La Liga': [x for x in range(1, 13)], 'Serie A': [x for x in range(1, 13)], 'Bundesliga': [x for x in range(1, 13)]})
    update_player_pos()