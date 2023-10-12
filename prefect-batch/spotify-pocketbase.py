# A script to scrape artists from Spotify API and write the data to PocketBase
import time
from datetime import timedelta
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from pocketbase import PocketBase
from prefect import task, flow
from prefect.tasks import task_input_hash
from prefect.blocks.system import Secret


# Prefect Secrets
spotify_id = Secret.load("spotify-jampify-id")
spotify_secret = Secret.load("spotify-jampify-secret")
pocketbase_db_url = Secret.load("pocketbase-jampify-url")
pocketbase_db_email = Secret.load("pocketbase-jampify-dev-email")
pocketbase_db_password = Secret.load("pocketbase-jampify-dev-password")


# Spotify
# flow: access_spotify_account -> search_artists_as_rows -> filter_artist_over_min_popularity
def access_spotify_account(client_id, client_secret) -> spotipy.Spotify:
    # access spotify account
    client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
    spotify_client = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    return spotify_client

@task(log_prints=True, retries=3 )
def search_artists_as_rows(spotify_client: spotipy.Spotify, years: list = [2000]) -> list:
    # get artists list from spotify api by year
    artist_list = []
    
    
    for year in years:

        query = f'year:{year}'
        try:
            start_time = time.time()

            results = spotify_client.search(q=query, type='artist')
            count = 1
            while results['artists']['next']:
                artist_list.extend(results['artists']['items'])
                
                results = spotify_client.next(results['artists'])
                count +=1
            
            end_time = time.time()
            print(f"scraping year: {year}, api calls: {count} in {end_time-start_time} seconds")

        except Exception:
            raise Exception

    return artist_list

def convert_spotify_artist_to_pb_row(spotify_artist_object: dict) -> dict:
    # change format of artist object
    temp_dict = {
        "spotify_id": spotify_artist_object['id'],
        "raw_json": spotify_artist_object,
        "artist_name": spotify_artist_object['name'],
        "popularity": spotify_artist_object['popularity'],
        "crawled_related_artists": False,
        "crawled_albums": False,
        "manual_notes": " ",
    }
    return temp_dict

def filter_artist_over_min_popularity(artist_list: dict, minimum_popularity: int=70) -> bool:
    # check if artist meets minimum popularity and change its format
    converted_list = []

    for artist in artist_list:
        if artist['popularity'] >= minimum_popularity:
            temp_dict = convert_spotify_artist_to_pb_row(artist)
            converted_list.append(temp_dict)

    return converted_list

# Database
## PocketBase
## flow: connect_to_pocketbase -> write_to_pocketbase
def connect_to_pocketbase(app_url: str, email: str, password: str) -> PocketBase:
    # connect to pocketbase
    client = PocketBase(app_url)
    admin_data = client.admins.auth_with_password(email, password)
    return client 

def check_for_duplicates(client: PocketBase, artist: str) -> bool:
    # check for duplicates in a pocketbase table named 'artists'
    try:
        results = client.collection('artists').get_list(query_params={'filter': f'spotify_id="{artist}"'})
        if results.total_items == 0:
            return True
        else:
            return False
    except Exception as error:
        raise error

#prefect task doc: https://docs.prefect.io/2.13.5/concepts/tasks/
@task(log_prints=True, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def write_to_pocketbase(client: PocketBase, artist_list: list):
    # write artist list to pocketbase
    print("writing to PocketBase..")
    for artist in artist_list:
        if check_for_duplicates(client, artist["spotify_id"]):
            try:
                client.collection('artists').create(artist)
            except Exception as e:
                print(e)
    print("done!")

@flow
def scraping_flow(year_start:int = 1900, year_range:int = 10, min_popularity:int = 50):
    # secrets
    client_id = spotify_id.get()
    client_secret = spotify_secret.get()
    app_url = pocketbase_db_url.get()
    email = pocketbase_db_email.get()
    password = pocketbase_db_password.get()

    # parameters
    values = range(year_start, year_start + year_range)
    years = [*values]

    # Spotify
    spotify_client = access_spotify_account(client_id, client_secret)
    artist_list = search_artists_as_rows(spotify_client, years)
    artist_list = filter_artist_over_min_popularity(artist_list, min_popularity)

    # Pocketbase
    client = connect_to_pocketbase(app_url, email, password)
    write_to_pocketbase(client, artist_list)


if __name__ == "__main__":

    year_start = 1990
    year_range = 2
    min_popularity = 70

    scraping_flow(year_start, year_range, min_popularity)