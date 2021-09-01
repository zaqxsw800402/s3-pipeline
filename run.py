""" Running ETL"""
import logging
import logging.config

import yaml


def main():
    config_path = "config/report1_config.yml"
    config = yaml.safe_load(open(config_path))
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info("This is test")


if __name__ == '__main__':
    main()
