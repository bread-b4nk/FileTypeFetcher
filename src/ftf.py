import json
import logging
import multiprocessing as mp
import os
from urllib.parse import urlparse

from commands import get_validated_args
from config import get_config_info
from web import download_and_ungzip, get_index_urls, save_file

PROCESS_TIMEOUT = 900  # 15 min timeout

tol = 1  # tolerance is 1 by default
tol_dict = {}  # it's the # of bad encounters with the hostname
# it's called tol_dict cause we use it for tolerance with hostnames


def run_batch(bcu, file_counts, out_dir, limit, cdx_name, config_dict):
    batch_cdx_urls = bcu
    num_procs = len(batch_cdx_urls)

    logging.info("New batch called, here's file counts and tolerance:")
    logging.info(file_counts)
    logging.info(tol_dict)

    # args = [file_counts, out_dir, limit, cdx_name, config_dict]
    #    repeated_args = list(chain.from_iterable( args * num_procs))
    itr = []

    for url in batch_cdx_urls:
        itr.append([url, file_counts, out_dir, limit, cdx_name, config_dict])

    with mp.Pool(processes=num_procs) as pool:
        res = pool.starmap(fetch_from_cdx, itr)

    for result in res:
        logging.debug("Got result " + str(result))
        if result != 0:
            logging.error("Something went wrong with a process")
            logging.error("Got exit code " + str(result))
            # but we don't return error in the hopes that the others work fine

    return 0


def fetch_from_cdx(cdx_url, f_counts, out_dir, limit, cdx_name, config_dict):
    file_counts = f_counts
    """
    file_counts = args[0]
    out_dir = args[1]
    limit = args[2]
    cdx_name = args[3]
    config_dict = args[4]
    """
    # unique name pulled from url
    cdx_num = cdx_url.split("/")[-1][:-3]
    tmp_name = cdx_num + ".gz"
    output_path = out_dir + cdx_name

    logging.debug("Calling download_and_ungzip on " + cdx_url)
    output = download_and_ungzip(tmp_name, cdx_url, output_path, cdx_num)

    # if we can't download a cdx_url, then it's implied that we won't get
    # any other cdx_url, so failing this one means we fail the
    # whole batch
    if output == "":
        logging.error("Download and unzip failed with url " + cdx_url)
        return 1

    cdx_path = output

    logging.debug("Opening " + cdx_path)
    cdx_file = open(cdx_path, "r")

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
                continue  # move onto next line in cdx_file

        except KeyError:
            continue  # move onto next line in cdx_file
        except Exception as err:
            logging.error("json.loads failed with " + parsed_line)
            logging.error(err)
            continue  # move onto next line in cdx_file

        # check tolerance with this url
        try:
            hostname = urlparse(cdx_url).hostname

            if tol_dict[hostname] > tol:
                continue  # move onto next line in cdx_file
        except KeyError:
            tol_dict[hostname] = 0
        except Exception as err:
            logging.error(err)

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
                logging.error(err)
                continue  # move onto next filetype

            dest = out_dir + filetype
            # see if config lines up
            if target_mime == cdx_mime:
                logging.debug("Saving " + cdx_url + " to " + dest + "...")
                # download image
                if save_file(cdx_url, dest, filetype) == 0:
                    file_counts[filetype] += 1
                    logging.debug("Saved " + cdx_url + " to " + dest)
                else:
                    logging.debug("Failed to save " + cdx_url)
                    hostname = urlparse(cdx_url).hostname
                    tol_dict[hostname] += 1

            elif target_mime is None and cdx_ext in target_exts:
                logging.debug("Saving " + cdx_url + " to " + dest + "...")

                dest = out_dir + filetype
                # download image
                if save_file(cdx_url, dest, filetype) == 0:
                    file_counts[filetype] += 1

                    logging.debug("Saved " + cdx_url + " to " + dest)
                else:
                    logging.debug("Failed to save " + cdx_url)
                    hostname = urlparse(cdx_url).hostname
                    tol_dict[hostname] += 1

        if all(count >= limit for count in file_counts.values()):
            logging.info("Downloaded sufficient files")
            logging.debug(file_counts)
            return 0

    cdx_file.close()
    return 0


def main():
    # set up logging
    logging.basicConfig(
        filename="ftf.log",
        filemode="w",
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s : %(message)s",
    )

    # get arguments
    logging.debug("Validating arguments")
    args = get_validated_args()

    # validate arguments
    if args == -1:
        logging.error("get_validated_args() failed")
        return 1

    # storing arg values in local variables
    limit = args.limit
    filetypes = args.filetypes
    num_procs = args.num_procs
    out_dir = args.output
    global tol
    tol = args.tolerance  # reinitialize to whatever user gave

    # get config info from config file
    config_dict = get_config_info()

    # error check config file
    if config_dict == {}:
        logging.error("get_config_info failed")
        return 1

    # get urls of index files by fetching from
    # "https://index.commoncrawl.org/collinfo.json"
    logging.debug("Getting urls to index files")
    index_urls = get_index_urls()

    # error check index urls
    if index_urls == {}:
        logging.error("get_index_urls() failed")
        return 1

    # using Manager() to have a dictionary as shared memory
    file_counts = mp.Manager().dict()
    # the dictionary will map file types to the
    # number of files downloaded

    logging.debug("Setting up directories for each extension")
    for extension in filetypes:
        file_counts[extension] = 0

        # initialize directories too
        if not os.path.exists(out_dir + extension):
            os.makedirs(out_dir + extension)

    logging.debug("Looping through urls and starting batches")
    for name in index_urls:
        if not os.path.exists(out_dir + name):
            os.makedirs(out_dir + name)

        logging.debug("Running download_and_ungzip on " + index_urls[name])

        result_dir = download_and_ungzip(
            "tmp.gz", index_urls[name], out_dir + name, "index.paths"
        )

        if result_dir == "":
            continue

        batch_cdx_urls = []

        try:
            index_paths = open(result_dir, "r")
        except Exception as err:
            logging.error("Couldn't open " + "'" + result_dir + "'")
            logging.error(err)
            return -1

        logging.debug("Parsing " + out_dir + name)

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
                urls = batch_cdx_urls
                lim = limit
                od = out_dir
                rc = run_batch(urls, file_counts, od, lim, name, config_dict)

                if rc != 0:
                    logging.error("run_batch failed, aborting...")
                    print("Something went wrong, check the log file (ftf.log)")
                    return 1

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
