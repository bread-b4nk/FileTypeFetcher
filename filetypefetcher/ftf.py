import multiprocessing as mp
from web import get_index_urls,download_cdx_urls
from commands import get_validated_args


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


    # dictionary of # of files for each filetype
    file_counts = {}
    for extension in filetypes:
        file_counts[extension] = 0

    for name in index_urls:

        result_dir = download_cdx_urls(name,index_urls[name],out_dir)
        
        index_paths = open(result_dir,"r")
        for line in index_paths:

            url = "https://data.commoncrawl.org/" + line
        
            
if __name__ == "__main__":
    main()
