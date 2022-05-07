from threading import Thread
import numpy as np
from googlesearch import search
PANORAMA_LINKS = ["panorama", "ryb.ru", "kolibri.press"]
PAUSE_SECONDS = 5

def is_panorama_title(query):
    query = '"' + query + '"'
    for link in search(query, tld='ru', lang = 'ru', stop=10, pause=PAUSE_SECONDS):
        if any([panorama_link in link for panorama_link in PANORAMA_LINKS]):
            return True
    return False

def query_job(subset, thread):
    for i, query in enumerate(subset):
        if i % 10 == 0:
            print(f'Completed {i} in Thread {thread}')
        to_query[query] = is_panorama_title(query)
            
    print(f'Finished Thread {thread}')

def split(a, n):
    k, m = divmod(len(a), n)
    return [a[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n)]

def main(num_threads, num_queries):
    # create threads
    work = list()
    subset_len = num_queries * num_threads
    subset = [k for k, v in to_query.items() if v is None][:subset_len]
    splitted = split(subset, num_threads)
    for item in splitted:
        work.append(item)
    threads = [Thread(target=query_job, args=(job, i)) for i, job in enumerate(work)]

    # start the threads
    for thread in threads:
        thread.start()

    # wait for the threads to complete
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    to_query = np.load(os.path.join(FILE_PARSING_FOLDER,'to_query.npy'), allow_pickle=True).item()

    try:
        main(1, 3000)
    except Exception:
        pass
    
    np.save('to_query', to_query)
    print(len([k for k,v in to_query.items() if v is not None]))
