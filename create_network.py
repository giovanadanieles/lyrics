import re
import html
import pronouncing
import numpy as np
import pandas as pd
import xnetwork as xn
import multiprocessing

from pathlib import Path
from tqdm.auto import tqdm
from multiprocessing import Pool
from nltk.tokenize import word_tokenize
from networkUtilities import createNetworksMultiScale


window_size = 11
k_expected = 20

data_folder = Path("data/")
network_path = data_folder/'network'/(("w%d_k%d")%(window_size,k_expected))
network_path.mkdir(parents=True, exist_ok=True)


def filter_data(df_songs):
    return df_songs[~(df_songs['lyrics'].isnull()) & (
                     (df_songs['language'] == 'eng') | (df_songs['language_detect'] == 'english'))]

def preprocess_lyrics(df_songs):
    #clean_reg = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')
    clean_reg = re.compile('<.*?>')

    def parse_lyrics_html(raw_html):
        return html.unescape(re.sub(clean_reg, ' ', raw_html.lower()))

    df_songs['lyrics'] = df_songs['lyrics'].fillna("")
    df_songs['lyrics'] = df_songs['lyrics'].apply(parse_lyrics_html)

    return df_songs

def load_data():
    file = data_folder/"wasabi_songs_without_genre_final.csv"

    dtypes = {'_id': 'str', 'position': 'Int64', 'lengthAlbum': 'str', 'lyrics': 'str', 
              'urlWikipedia': 'str', 'id_album': 'str', 'isClassic': 'bool', 'title': 'str',
              'publicationDateAlbum': 'str', 'albumTitle': 'str', 'isrc': 'str', 
              'length': 'Int64', 'explicitLyrics': 'str', 'rank': 'Int64', 
              'bpm': 'float64', 'gain': 'float64', 'preview': 'str', 'publicationDate': 'str', 
              'language': 'str', 'language_detect': 'str','name': 'str', 
              'explicit_content_lyrics': 'Int64'}

    df_songs = pd.read_csv(file, sep='\t', decimal='.', dtype=dtypes)
    
    df_songs = filter_data(df_songs)
    df_songs = preprocess_lyrics(df_songs)
    
    return df_songs

def get_word_phones(tokens):
    phones = []
    
    for t in tokens:
        word_phones = pronouncing.phones_for_word(t)
        
        if len(word_phones) != 0:
            phone = word_phones[0]
            phone = re.split(' ', phone.strip())
            phones += phone
            
    return phones

def delete_edges_window_criteria(g):
    delete = []
    
    for pos,e in enumerate(g.get_edgelist()):
        if (abs(e[0]-e[1]) > 1 and abs(e[0]-e[1]) < window_size):
            delete.append(pos)
            
        if abs(e[0]-e[1]) == 1:
            g.es[pos]['weight'] = 1.
            
    g.delete_edges(delete)
    
    return g

def delete_edges_k_criteria(g):
    weight_ordered = np.argsort(g.es['weight'])[::-1]
    
    delete = weight_ordered[g.vcount()*k_expected//2::].tolist()
    g.delete_edges(delete)
    
    return g

def build_network_from_data(song_id_and_lyrics):
    try:
        song_id,lyrics = song_id_and_lyrics
        song_network_path = network_path/(song_id+'.xnet')
        
        if song_network_path.is_file():
            return 
        
        tokens = word_tokenize(lyrics)
        phones = get_word_phones(tokens)

        g = createNetworksMultiScale(phones, window_size)
        g.vs["phones"] = phones[window_size//2+1:g.vcount()+window_size//2+1]

        g = delete_edges_window_criteria(g)
        g = delete_edges_k_criteria(g)
        
        xn.igraph2xnet(g, song_network_path)
        
        return song_id,g
    
    except Exception as e:
        print("Error with: ", song_id)
        return

if __name__ == '__main__':
	df_songs = load_data()

	arglist = list(zip(df_songs['_id'], df_songs['lyrics']))
	num_processors = multiprocessing.cpu_count()

	pool = Pool(processes=num_processors)
	for result in tqdm(pool.imap(func=build_network_from_data, iterable=arglist), total=len(df_songs)):
	    pass

	pool.close()
	pool.terminate()	                            