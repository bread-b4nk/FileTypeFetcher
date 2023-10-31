import json
import logging
import os


# tries to open config file (filetype_config.json) which should be in src
def get_config_info(config_filename):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(dir_path, config_filename)
    logging.debug("Opening " + config_path)
    try:
        config = open(config_path, "r")
    except Exception as err:
        logging.error(
            "Couldn't open config file \
            expected it to be at "
            + config_path
        )
        logging.error(err)
        return {}

    # parse config data
    try:
        config_dict = json.loads(config.read())
    except Exception as err:
        logging.error("Couldn't parse config file")
        logging.error(err)
        config.close()
        return {}

    config.close()
    return config_dict
