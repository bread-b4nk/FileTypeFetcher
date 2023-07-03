import requests
import json
import gzip
import os
"""
we want: https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-23/cc-index.paths.gz

they give: https://index.commoncrawl.org/CC-MAIN-2023-14-index
"""

# change this url if they move the json file for index links
INDEX_FILES_URL = "https://index.commoncrawl.org/collinfo.json"
INDEX_PATHS_URL = "https://data.commoncrawl.org/crawl-data/ID/cc-index.paths.gz"


def gzip_extract(source_path,dest_path,block_size=65536):
    source = gzip.open(source_path,"rb")
    dest = open(dest_path,"wb")
    
    while True:
        block = source.read(block_size)
        if not block:
            break
        else:
         dest.write(block)
    
    source.close()
    dest.close()

def get_index_urls():
    try:
        response = requests.get(INDEX_FILES_URL,timeout=5)
    except Exception as err:
        print(err)
        print("response.get from response library failed")
        return {}

    if response.status_code != 200:
        print("reponse.get got a status code of " + str(response.status_code))
        return {}

    # should error check index response
    try:
        json_data = json.loads(response.content)
    except Exception as err:
        print(err)
        print("json.loads from json library failed")
        return {}

    # store dict from name of index file to url
    out = {}

    # json data had column headings of:
    # "name" and "cdx-api"
    prefix = INDEX_PATHS_URL.split("ID")[0]
    suffix = INDEX_PATHS_URL.split("ID")[1]

    for row in json_data:
        out[row["name"].replace(" ","_").replace("/","_")] = prefix + row["id"] + suffix

    return out

def download_cdx_urls(name,url,out_dir):
    
    try:
        response = requests.get(url,timeout=5)
    except Exception as err:
        print(err)
        print("response.get from response library failed")
        return ""

    if response.status_code != 200:
        print("response.get got a status code of " + str(response.status_code))
        return ""
    
    if not os.path.exists(out_dir + name):
        os.makedirs(out_dir+name)

    # path to output file
    gz_path = out_dir + name + ".gz"

    # write gzip to file
    gz = open(gz_path,"wb")
    gz.write(response.content)

    gz.close()
   
    # extract gzip
    gzip_extract(gz_path, out_dir+name + "/" +"index.paths")
    
    # delete gzip
    os.remove(gz_path)
    
    # return path to unzipped
    return out_dir+name+"/"+"index.paths"
