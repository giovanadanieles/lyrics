import sys
import time
import ujson
import requests
import pandas as pd

from tqdm.auto import tqdm
from collections import defaultdict


def create_request_uri(spec, mode, fields):
    base_url = "https://wasabi.i3s.unice.fr/api/v1"
    params = ""
    
    if mode == "song": # spec is the song id
        operation_path = "song/id"
    elif mode == "album": # spec is the album id
        operation_path = "album/id"
    else: # spec is the start index [0, 2099286]
        operation_path = "song_all"
        params = "?project=" + ','.join(fields) 
    
    return '/'.join([base_url, operation_path, str(spec), params])

def get_request_text(spec, session, mode="song", fields=None):
    request_uri = create_request_uri(spec, mode, fields)
    
    sleep_time = 0
    
    while True:
        rqst = session.get(request_uri).text
        
        if rqst == "Too many requests, please try again later.":
            time.sleep(sleep_time + 10)
        else:
            break

    return rqst

def get_filtered_infos(songs_batch, params):
    songs_info = ujson.loads(songs_batch)

    if str(type(songs_info)) != "<class 'list'>":
        songs_info = [songs_info]

    return [[song[p] if p in song.keys() else "" for p in params] for song in songs_info]

def get_song_genres(song_id, session): # first try to find the genre of the song; if not found, 
                                       #try the album's genre; if not found, return empty  
    rqst = get_request_text(song_id, session, "song")
    song_genre, album_id = get_filtered_infos(rqst, ['genre', 'id_album'])[0]

    if song_genre == "" and album_id != "":
        if album_id in album_genres:
            return album_genres[album_id]
        
        rqst = get_request_text(album_id, session, "album")
        song_genre = get_filtered_infos(rqst, ['genre'])[0]
        
        album_genres[album_id] = song_genre

    return song_genre

def build_dataset(left_range, right_range):
    rqst_params = rqst_params = ['_id', 'position', 'lengthAlbum', 'lyrics', 'urlWikipedia', 'id_album', 'isClassic', 
                   'title', 'publicationDateAlbum', 'albumTitle', 'deezer_mapping', 'id_song_deezer',
                   'isrc', 'length', 'explicitLyrics', 'rank', 'bpm', 'gain', 'preview', 'publicationDate',
                   'urlITunes', 'urlSpotify', 'urlYouTube', 'urlAmazon', 'urlLastFm', 'language',
                   'id_artist_deezer', 'id_album_deezer', 'urlDeezer', 'language_detect', 'name',
                   'title_accent_fold', 'explicit_content_lyrics', 'chords_metadata']

    df_songs = pd.DataFrame(columns=rqst_params)

    for start_idx in tqdm(range(left_range, right_range, 200)):
        session = requests.Session()
        
        rqst = get_request_text(start_idx, session, "all", rqst_params)

        songs = get_filtered_infos(rqst, rqst_params)    
        genres = [get_song_genres(s[0], session) for s in songs]   

        for idx in range(len(songs)):
            songs[idx].append(genres[idx])

        df_songs = df_songs.append(pd.DataFrame(songs, columns=rqst_params + ['genres']), ignore_index=True)  

        if start_idx > 0 and start_idx % 200 == 0:
            df_songs.to_csv(f'wasabi_songs_{start_idx}.csv', sep='\t', encoding='utf-8', index=False) 

    df_songs.to_csv('wasabi_songs.csv', sep='\t', encoding='utf-8', index=False)       


album_genres = defaultdict(str)

if len(sys.argv) != 3:
	print("Wrong arguments. Please provide a left and a right range for the crawler.")
else:
	build_dataset(int(sys.argv[1]), int(sys.argv[2]))