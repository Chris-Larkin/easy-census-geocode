import pandas as pd
import numpy as np
import os
import io
import random
import math
import censusgeocode
import time
import pickle
import random
import threading
from pathlib import Path
from IPython.display import display, clear_output
from concurrent.futures import ThreadPoolExecutor

# parameters

## size of chunk to geocode
chunk_size = 150 

## size of chuck to geocode in the end for addresses that failed
errors_chunck_size = 150

## number of workers to parallelize requests
max_workers = 16

k_columns = ['num_street','city','state','zip_code']

# cache of the results
cache_lock = threading.Lock() # to lock access to the cache file
cachefile = Path('.') / 'cache-dict.csv'
cache = {}
cache_df = None
if cachefile.exists():
    cache = pickle.loads(cachefile.read_bytes())
    ds = []
    for k,v in cache.items():
        d = {}
        d.update({k:v for k,v in zip(k_columns, k)})
        d.update(v)
        ds.append(d)
    cache_df = pd.DataFrame(ds)

def safe_print(*args, sep=" ", end="", **kwargs):
    '''
    more thread safe print()
    '''
    
    joined_string = sep.join([str(arg) for arg in args])
    print(joined_string  + "\n", sep=sep, end=end, **kwargs)

## change directory and filename
df_original = pd.read_csv('/Users/Directory/file_name.csv', dtype={'id': 'str'})

# make sure there is no duplicated id
assert df_original['id'].duplicated().sum() == 0

df = df_original.copy()
df = df.set_index(k_columns)
for c in ['address','match','matchtype','parsed','tigerlineid','side','statefp','countyfp','tract','block','lat','lon']:
    df[c] = None
if cache:
    df.update(cache_df.set_index(k_columns))
df = df.reset_index().set_index('id')

df_with_cache = df
df = df_with_cache[df['match'].isnull()].copy()

n_chunks = math.ceil(df.shape[0] / chunk_size)

print('Total rows:', df.shape[0])
print('Chunk size:', chunk_size)
print('Number of chunks:', n_chunks)








