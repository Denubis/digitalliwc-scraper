#!/usr/bin/env python3
# Read search_terms.json, fetch a search result using the query, cursor through the search result, build an array of object ids, then fetch all data from the object ids and export to csv via a dataframe.

import requests
import json
import pandas as pd
import time
from tqdm import tqdm
from pprint import pprint
import os
import shutil
import re
import textwrap

def sanitize_filename(filename):
    return re.sub(r'[<>:"/\\|?*]', '_', filename)


OUTDIR="output/"
if os.path.exists(OUTDIR):
    shutil.rmtree(OUTDIR, ignore_errors=True)
os.makedirs(OUTDIR) 


SEARCH_URL="https://www.salsah.org/api/search/?searchtype={searchtype}&filter_by_project={filter_by_project}&show_nrows=50&start_at={startat}&lang=en&filter_by_restype={filter_by_restype}&property_id%5B%5D={property_id}&compop%5B%5D={compop}&searchval%5B%5D={searchval}"

DETAILS_URL="https://www.salsah.org/api/graphdata/{object_id}?full=1&lang=en"

def fetch_objects_from_search(search_terms):
    """Given a dictionary, search the search url, cursor through results and emit a list of object ids"""

    searchtype = search_terms['searchtype']
    filter_by_project = search_terms['filter_by_project']
    filter_by_restype = search_terms['filter_by_restype']
    property_id = search_terms['property_id']
    compop = search_terms['compop']
    searchval = search_terms['searchval']
    startat=0

    print(f"{searchval=} {searchtype=} {filter_by_project=} {filter_by_restype=} {property_id=} {compop=} {startat=}")

    object_ids = []

    # Fetch the search results
    # first query to build our pager dictionary
    

    def parse_objects(result):
        local_object_ids = [] 
        for object in result['subjects']:
            local_object_ids.append(object['obj_id'].replace("_-_local",""))
        return local_object_ids

    def get_page(startat, searchtype, filter_by_project, filter_by_restype, property_id, compop, searchval):
        # print(f"Getting page {startat}\n\t{searchtype=} {filter_by_project=} {filter_by_restype=} {property_id=} {compop=} {searchval=}")
        search_url = SEARCH_URL.format(searchtype=searchtype, filter_by_project=filter_by_project, filter_by_restype=filter_by_restype, property_id=property_id, compop=compop, searchval=searchval, startat=startat)
        # print(search_url)
        result_raw = requests.get(search_url)
        result = result_raw.json()
        paging = result['paging']
        objects = parse_objects(result)
        nhits = result['nhits']
        local_paging = [x for x in result['paging'] if not x['current']]
        # print(local_paging)
        return objects, local_paging, nhits



    objects, paging, nhits = get_page(0, searchtype, filter_by_project, filter_by_restype, property_id, compop, searchval)

    object_ids.extend(objects)

    pprint(nhits)
    # make tqdm progress bar for nhits, we will update in while loop
    pbar = tqdm(total=int(nhits))


    while paging:    
        # break    
        # fetch the next page
        # pop first dict off paging
        startat = paging.pop(0)['start_at']

        # update prbar with startat
        
        pbar.update(startat or nhits)
        objects, _, nhits = get_page(startat, searchtype, filter_by_project, filter_by_restype, property_id, compop, searchval)
        object_ids.extend(objects)

    pbar.update(int(nhits))
    pbar.close()
    # pprint(object_ids)
    return object_ids


def get_objects_from_ids(object_ids):
    """Given a list of object ids, fetch the details for each object and return a list of objects"""
    objects = []
    for object_id in tqdm(object_ids, desc="Fetching objects"):
        details_url = DETAILS_URL.format(object_id=object_id)
        # print(details_url)
        result_raw = requests.get(details_url)
        result = result_raw.json()
        # pprint(result, width=200)

        flattened_object = {}
        handle_id = result['graph']['nodes'][object_id]['resinfo']['handle_id']

        liwc_catalog = []
        for node in result['graph']['nodes']:            
            sub_node = [] 
            if result['graph']['nodes'][node]['resinfo']['label'] == 'Catalog LIMC':
                for property in result['graph']['nodes'][node]['properties']:
                    value = ' '.join(result['graph']['nodes'][node]['properties'][property]['values'])                    
                    sub_node.append(value)
            if sub_node:
                liwc_catalog.append('_'.join(sub_node))
            
        # print(f"{liwc_catalog=}")
        
        monument_type = result['graph']['nodes'][object_id]['properties']['limc:object']['values'][0]

        filename = f"{object_id}-{sanitize_filename(monument_type)}-{textwrap.shorten(sanitize_filename('+'.join(liwc_catalog)),width=200, placeholder='+++')}.txt"

        with open(f"{OUTDIR}/{filename}", "w") as f:
            f.write(f"http://ark.dasch.swiss/{handle_id}\n")
            for cat in liwc_catalog:
                f.write(f"{cat}\n")
            f.write("\n\n\n")
            for node in result['graph']['nodes']:
                # print(f"{node=}")
                label = result['graph']['nodes'][node]['resinfo']['label']
                f.write(f"{label}\n")
                for property in result['graph']['nodes'][node]['properties']:
                    # print(f"{property=}")
                    value = ' '.join(result['graph']['nodes'][node]['properties'][property]['values'])
                    property_label = result['graph']['nodes'][node]['properties'][property]['label']
                    f.write(f"\t{property_label}:\t{value}\n")



        objects.append({'filename': filename, 'monument_type':monument_type, 'liwc_catalog':';'.join(liwc_catalog), 'handle_id':f"http://ark.dasch.swiss/{handle_id}", 'object_id':object_id})
        # break
    return objects

def main():
    objects = []
    with open('search_terms.json') as json_file:
        search_terms = json.load(json_file)
        for search_term in search_terms:
            object_ids = fetch_objects_from_search(search_term)
            objects.extend(get_objects_from_ids(object_ids))
            # break

    df = pd.DataFrame(objects)
    df.to_csv('objects.csv')


if __name__ == "__main__":
    main()



