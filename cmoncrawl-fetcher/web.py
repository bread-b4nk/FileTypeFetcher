import gzip
import hashlib
import json
import logging
import os
import time

import requests

"""
we want urls in this format:
https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-23/cc-index.paths.gz
they give:
https://index.commoncrawl.org/CC-MAIN-2023-14-index
"""

# change this url if they move the json file for index links
INDEX_FILES_URL = "https://index.commoncrawl.org/collinfo.json"
index_host = "https://data.commoncrawl.org/"
index_path = "crawl-data/ID/cc-index.paths.gz"
INDEX_PATHS_URL = index_host + index_path

READ_BLOCK_SIZE = 65536


def gzip_extract(source_path, dest_path, block_size=READ_BLOCK_SIZE):
    try:
        source = gzip.open(source_path, "rb")
        dest = open(dest_path, "wb")

        while True:
            block = source.read(block_size)
            if not block:
                break
            else:
                dest.write(block)

        source.close()
        dest.close()
    except Exception as err:
        logging.error("gzip_extract failed with " + source_path)
        logging.error(err)
        return 1

    return 0


"""
- Pulls json from <INDEX_FILES_URL>
- Reformats text and returns a dictionary
  pointing the "name" of the index to its url

"""


def get_index_urls():
    try:
        response = requests.get(INDEX_FILES_URL, timeout=5)
    except Exception as err:
        logging.error("requests.get failed to get " + INDEX_FILES_URL)
        logging.error(err)
        return {}

    if response.status_code != 200:
        logging.error(
            "requests.get got a status code of "
            + str(response.status_code)
            + " for url: "
            + INDEX_FILES_URL
        )

        return {}

    # should error check index response
    try:
        json_data = json.loads(response.content)
    except Exception as err:
        logging.error("json couldn't parse content in " + INDEX_FILES_URL)
        logging.error(err)
        return {}

    # store dict from name of index file to url
    out = {}

    # json data had column headings of:
    # "name" and "cdx-api"
    prefix = INDEX_PATHS_URL.split("ID")[0]
    suffix = INDEX_PATHS_URL.split("ID")[1].strip("\n")

    for row in json_data:
        out[row["name"].replace(" ", "_").replace("/", "_")] = (
            prefix + row["id"] + suffix
        )

    return out


"""
- Downloads a file from <url> and stores it into <out_dir>/<tmp_name>.gz
  (we assume the file downloaded is a gzip file)
- Unzips it into <out_dir>/<output_file_name>
- We use <tmp_name> as the name of the .gz file we download
  then delete
- We return the path to the resulting file

We assume out_dir does NOT have a trailing "/"
"""


def download_and_ungzip(tmp_name, url, out_dir, output_file_name):
    try:
        response = requests.get(url.strip(" ").strip("\n"))
    except Exception as err:
        logging.error("requests.get failed with " + url.strip(" ").strip("\n"))
        logging.error(err)
        return ""

    # if we get the slowdown warning, sleep then exit
    if response.status_code == 503:
        logging.debug("Let's slow down, sleeping Zzz...")
        time.sleep(8)
        return ""

    if response.status_code != 200:
        logging.error("requests.get got status " + str(response.status_code))
        return ""

    # path to output file
    gz_path = out_dir + "/" + tmp_name

    # write gzip to file
    gz = open(gz_path, "wb")

    logging.debug("url: " + url)
    gz.write(response.content)

    gz.close()

    # resulting unzipped file
    output_file_path = out_dir + "/" + output_file_name

    # extract gzip
    if gzip_extract(gz_path, output_file_path) != 0:
        return ""

    # delete gzip
    os.remove(gz_path)

    # return path to unzipped
    return output_file_path


"""
- Saves the file to the right directory
(based on file type)
- Filename will be its md5 hash

Will return -1 if fails, returns the path to the
output file if successful
"""


def save_file(url, out_dir, filetype):
    logging.debug("Downloading " + url + " to " + filetype + " directory")

    if out_dir[-1] != "/":
        out_dir += "/"

    try:
        response = requests.get(url)
    except Exception as err:
        logging.error("requests.get failed with " + url)
        logging.error(err)
        return -1

    if response.status_code != 200:
        logging.error(
            "requests.get got a status code of "
            + str(response.status_code)
            + " for url: "
            + INDEX_FILES_URL
        )
        return -1

    if url.split("/")[-1] == "":  # if no info after hostname
        tmp_filename = out_dir + "tmp"
    else:
        tmp_filename = out_dir + url.split("/")[-1]

    logging.debug("Temporary filename: " + tmp_filename)

    # write bytes to a temporary file
    tmp_write = open(tmp_filename, "wb")
    tmp_write.write(response.content)
    tmp_write.close()

    # generate hash from file
    tmp_read = open(tmp_filename, "rb")

    md5_hash = hashlib.md5()
    binary = tmp_read.read(READ_BLOCK_SIZE)
    while len(binary) > 0:
        md5_hash.update(binary)
        binary = tmp_read.read(READ_BLOCK_SIZE)

    tmp_read.close()

    os.remove(tmp_filename)

    filename = md5_hash.hexdigest()

    logging.debug("Writing file to " + out_dir + filename + "." + filetype)

    # save image with the md5 as its name
    out_file = open(out_dir + filename + "." + filetype, "wb")
    out_file.write(response.content)
    out_file.close()

    return 0
