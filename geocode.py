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


def addressbatch_retry(i, data, sleep=30, att=3):
    '''
    call cg.addressbatch() and retry 'att' times if it fails
    '''
    cg = censusgeocode.CensusGeocode()
    time.sleep(random.randint(1,40) / 10)
    data = data.read()
    while att > 0:
        try:
            att -= 1
            r = cg.addressbatch(io.StringIO(data), timeout=8*60)
            return r
        except Exception as ex:
            safe_print('ERROR:', ex)
            if att > 0:
                safe_print(f'Waiting {sleep} secs before trying again. Remaing {att} attempts.')
                time.sleep(sleep)
                sleep *= 2
    return None



def geocode(i):
    cdf = df[i*chunk_size:(i+1)*chunk_size]
    
    safe_print(f'geocoding chunk {i+1}/{n_chunks}')
    
    istart = time.perf_counter()
    k = addressbatch_retry(i, io.StringIO(cdf.to_csv(header=False)))
    iend = start_time = time.perf_counter()
    
    if k:
        safe_print(f'chunk {i+1} OK. Partial: {iend-istart:.2f} secs. Total: {(iend-start)/60:.2f} mins')
        
        # include results in the cache
        with cache_lock:
            r_df = pd.DataFrame(k)
            r_df = r_df.set_index('id')
            m = df[k_columns].merge(r_df, on='id')
            cache.update(m.drop_duplicates(k_columns).set_index(k_columns).to_dict('index'))
            cachefile.write_bytes(pickle.dumps(cache))
        return k
    else:
        safe_print(f'chunk {i+1} FAILED. Partial: {iend-istart:.2f} secs. Total: {(iend-start)/60:.2f} mins')
        return None
    
    
    
results = []
start = time.perf_counter()
with ThreadPoolExecutor(max_workers=16) as executor:
    results = executor.map(geocode, range(n_chunks))
    
    
    
results = list(results)

# dataframe with results
results_df = pd.concat(map(lambda r: pd.DataFrame(r).set_index('id'), filter(lambda e: e is not None, results)))

# update dataframe with results
df.update(results_df)

fdf = df_with_cache.copy()
fdf.update(df)

# select rows that were not geocoded
errors_df = fdf[fdf['match'].isnull()]

print(f'geocoding {errors_df.shape[0]} failed addresses')

errors_n_chuncks = math.ceil(errors_df.shape[0] / 50)
results = []
for i in range(errors_n_chuncks):
    data = errors_df.iloc[i*50:(i+1)*50][['num_street','city','state','zip_code']].to_csv(header=False)
    k = addressbatch_retry(i, io.StringIO(data))
    
    if k:
        print(f'{i+1}/{errors_n_chuncks} OK')
        results.append(k)
        
        print('len cache:', len(cache))
        r_df = pd.DataFrame(k)
        r_df = r_df.set_index('id')
        m = df[k_columns].merge(r_df, on='id')
        cache.update(m.drop_duplicates(k_columns).set_index(k_columns).to_dict('index'))
        cachefile.write_bytes(pickle.dumps(cache))
        print('len cache:', len(cache))
    else:
        print(f'{i+1}/{errors_n_chuncks} FAILED')
        
        
 
# update dataframe
if results:
    errors_results_df = pd.concat(map(lambda r: pd.DataFrame(r).set_index('id'), results))
    fdf.update(errors_results_df)
    
    
    
# save results
fdf.to_csv('final.csv')

