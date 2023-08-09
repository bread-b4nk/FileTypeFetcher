# FileTypeFetcher (FTF)
Downloads desired filetypes from Common Crawl data

# Usage

```
usage: ftf.py [-h] -l <limit> -f FILETYPES [FILETYPES ...] [-p NUM_PROCS] -o OUTPUT [-t TOLERANCE]

Python package that downloads files from common crawler's database. An example usage is `ftf.py -l 5 -f jpg png -o out_dir` This'll make it download 5 jpgs and 5 pngs into out_dir

options:
  -h, --help            show this help message and exit
  -l <limit>, --limit <limit>
                        Number of images per filetype desired
  -f FILETYPES [FILETYPES ...], --filetypes FILETYPES [FILETYPES ...]
                        Desired filetypes to fetch
  -p NUM_PROCS, --num_procs NUM_PROCS
                        Number of processes to use, default is 1
  -o OUTPUT, --output OUTPUT
                        Output directory to store downloaded files
  -t TOLERANCE, --tolerance TOLERANCE
                        Number of fails for a given hostname before we ignore this host
```

Required arguments are the desired file type (input the extension), the output directory, and the number of desired files for each extension.

By default we prioritize those with Content Types that signify the filetype over the extension, we store corresponding content types for file types in `filetype_config.json`, we currently support 69 file types.

# Contributing

- Package installation uses [poetry](https://python-poetry.org/docs/basic-usage/), but this is subject to change in the future. 

- We use [git-flow workflows](https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow) for our development. Depending on the kind of feature you are contributing, create a `hotfix` branch or a `feature` branch. Installation instructions are [here](https://github.com/nvie/gitflow/wiki/Installation).

- We also require a pre-commit hook. You can follow the instructions [here](https://pre-commit.com/#install) to install them.

- You will need to install the hooks in the `yaml` file in the repository using the following command: `pre-commit install`.

# License

We've released this project under GPLv3. Check the LICENSE file for more details.
