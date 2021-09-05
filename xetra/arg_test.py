import argparse

import yaml

parser = argparse.ArgumentParser(description='Run the Xetra ETL Job.')
parser.add_argument('config', help='A configuration file in YAML format.')
args = parser.parse_args()
config = yaml.safe_load(open(args.config))
# log_config = config['logging']
s3_config = config['s3']
print(s3_config)