import json
import multiprocessing as mp
import os

from commands import get_validated_args
from web import download_and_ungzip, get_index_urls, save_file

CONFIG_FILE_PATH = "filetype_config.json"


def run_batch(batch_cdx_urls, file_counts, out_dir, limit, cdx_name):
    processes = []

    # spawn each process
    # and have it run the download and extract func
    for each_cdx_url in batch_cdx_urls:
        process = mp.Process(
            target=fetch_files_in_cdx,
            name=each_cdx_url,
            args=(each_cdx_url, file_counts, out_dir, limit, cdx_name),
        )
        process.start()
        processes.append(process)

    # join processes afterward
    for process in processes:
        return_code = process.join()
        if return_code != 0:
            print(
                "Process for "
                + process.name
                + " returned with code "
                + str(return_code)
            )


def fetch_files_in_cdx(cdx_url, file_counts, out_dir, limit, cdx_name):
    # unique name pulled from url
    cdx_num = cdx_url.split("/")[-1][:-3]
    tmp_name = cdx_num + ".gz"
    output_path = out_dir + cdx_name

    output = download_and_ungzip(tmp_name, cdx_url, output_path, cdx_num)

    if output == "":
        print("Download and unzip failed with url " + cdx_url)
        return -1

    cdx_path = output

    cdx_file = open(cdx_path, "r")

    # get data from config file
    config = open(CONFIG_FILE_PATH, "r")

    try:
        config_dict = json.loads(config.read())
    except Exception as err:
        print(err)
        print("Couldn't read config file.")
        print("Expected it to be at" + CONFIG_FILE_PATH)
        return -1

    config.close()

    for line in cdx_file:
        parsed_line = "{" + line.split("{")[1]
        # parse the cdx line
        # for content type, url, and extension
        try:
            cdx_data = json.loads(parsed_line)
            cdx_mime = cdx_data["mime-detected"]
            cdx_url = cdx_data["url"]
            cdx_ext = "." + cdx_url.split(".")[-1]

            if cdx_data["status"] != "200":
                continue

        except KeyError:
            continue
        except Exception as err:
            print(err)
            print("json.loads from json library failed with " + parsed_line)
            continue

        # loop through our desired file types
        for filetype in file_counts.keys():
            if file_counts[filetype] >= limit:
                continue  # move onto next filetype

            # see if we have a config for it
            try:
                info = config_dict[filetype]
                target_mime = info["mime-detected"]
                target_exts = info["ext"]

            except KeyError:
                target_mime = None
                target_exts = [filetype]
            except Exception as err:
                print(err)
                continue  # move onto next filetype

            # see if config lines up
            if target_mime == cdx_mime:
                # download image
                if save_file(cdx_url, out_dir + filetype, filetype) == 0:
                    file_counts[filetype] += 1
            elif target_mime is None and cdx_ext in target_exts:
                # downlad file to the right folder
                if save_file(cdx_url, out_dir + filetype, filetype) == 0:
                    file_counts[filetype] += 1

        if all(count >= limit for count in file_counts.values()):
            return 0

    cdx_file.close()
    return 0


def main():
    # get arguments
    args = get_validated_args()

    # check if function failed
    if args == -1:
        print("get_validated_args() failed")
        return -1

    # storing arg values in local variables
    limit = args.limit
    filetypes = args.filetypes
    num_procs = args.num_procs
    out_dir = args.output

    # get urls of index files
    index_urls = get_index_urls()

    # error check index urls
    if index_urls == {}:
        print("get_index_urls() failed")
        return -1

    # using Manager() to have a dictionary as shared memory
    file_counts = mp.Manager().dict()
    # the dictionary will map file types to the
    # number of files downloaded
    for extension in filetypes:
        file_counts[extension] = 0

        # initialize directories too
        if not os.path.exists(out_dir + extension):
            os.makedirs(out_dir + extension)

    for name in index_urls:
        if not os.path.exists(out_dir + name):
            os.makedirs(out_dir + name)

        result_dir = download_and_ungzip(
            "tmp.gz", index_urls[name], out_dir + name, "index.paths"
        )

        batch_cdx_urls = []

        try:
            index_paths = open(result_dir, "r")
        except Exception as err:
            print(err)
            return -1

        # looping through each .gz file in the index.paths file
        for url_suffix in index_paths:
            url_suffix = url_suffix.strip("\n")

            # sometimes we get non-gz files, we don't want those
            if url_suffix[-3:] != ".gz":
                continue

            url = "https://data.commoncrawl.org/" + url_suffix

            # add the url to our batch
            batch_cdx_urls.append(url)

            # once we reach batch number of urls, spawn processes
            if len(batch_cdx_urls) >= num_procs:
                run_batch(batch_cdx_urls, file_counts, out_dir, limit, name)

                # reset batch urls for next batch
                batch_cdx_urls = []

            # check if we're done
            if all(count >= limit for count in file_counts.values()):
                return 0

        index_paths.close()
        # os.remove(result_dir)
    return 0


if __name__ == "__main__":
    main()
