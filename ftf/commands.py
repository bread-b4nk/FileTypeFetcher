import multiprocessing as mp
import argparse
from os import path

def init_parser():

    parser = argparse.ArgumentParser(
    description = "Python package that downloads files from common crawler's database. Only a few extensions are currently supported"
    )

    parser.add_argument(
        "-l",
        "--limit",
        help = "Number of images per filetype desired",
        required = True,
        metavar = "<limit>",
        type = int
    )

    parser.add_argument(
        "-f",
        "--filetypes",
        help = "Desired filetypes to fetch",
        required = True,
        nargs = "+",
        type = str
    )

    parser.add_argument(
        "-p",
        "--num_procs",
        help = "Number of processes to use, default is 1",
        required = False,
        default = 1,
        type = int
    )

    parser.add_argument(
        "-o",
        "--output",
        help = "Output directory to store downloaded files",
        required = True,
        type = str
    )

    return parser

# returns 0 on success, returns 1 on fail
def validate_args(args):
    
    if args.limit <= 0:
        print("Limit (-l,--limit) must be a positive number")
        return 1
    
    # if they want more processes than what's detected
    if args.num_procs > mp.cpu_count():
        print("Too many processes, multiprocessing library detected only " + str(mp.cpu_count()) + " cores")
        return 1

    if not path.isdir(args.output):
        print("Expected output (-o,--output) to be an existing directory")
        return 1
    
    return 0

def main():
    
    # initialize parser and parse arguments using argparse library
    parser = init_parser()
    args = parser.parse_args()
    
    validate_args(args)
    
    limit = args.limit
    filetypes = args.filetypes
    num_procs = args.num_procs

    
    # dict of # of files per filetype
    file_count = {}
    for ext in filetypes:
        file_count[ext] = 0


if __name__ == "__main__":
    main()
