import argparse
import json
import logging
import os

logger = logging.getLogger(__name__)


def main():
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--debug", action="store_true", help="Enable Debugging")
    parser.add_argument('-c', '--conf', required=True, help="JSON Config File")
    parser.add_argument('-p', '--params', required=True, type=json.loads, help="JSON parameters")  # JSON parser
    args = parser.parse_args()

    # configure logging
    logging.basicConfig(format="%(asctime)s [%(name)s:%(lineno)d][%(funcName)s][%(levelname)s] %(message)s")

    # enable debugging
    if args.debug:
        logger.setLevel(logging.DEBUG)

    # open config and parse
    with open(args.conf) as data_file:
        conf = json.load(data_file)

    # check for project_path
    if 'project_path' not in conf:
        raise Exception("missing 'project_path' in conf")

    # check for script
    if 'script' not in conf:
        raise Exception("missing 'script' in conf")

    # parse enviroment variables
    envs = ""
    if 'env' in conf:
        envs = " ".join(conf['env'])

    # parse spark-submit parameter variables
    params = ""
    if 'params' in conf:
        params = " ".join(conf['params'])

    # parse pyfiles variables
    pyfiles = ""
    if 'pyfiles' in conf:
        files = [conf['project_path'] + "/" + p for p in conf['pyfiles']]
        pyfiles = "--py-files %s" % ",".join(files)

    # iterate paramters
    for r in args.params:
        cmd = "{} && spark-submit {} {} {}/{}".format(envs, params, pyfiles, conf['project_path'], conf['script'])
        cmd = cmd.format(**r)  # add parameter
        os.system(cmd)


if __name__ == "__main__":
    main()
