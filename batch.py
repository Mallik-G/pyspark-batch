#!/usr/bin/env python
# Copyright (c) 2017 Alexander Hurd
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
import argparse
import json
import logging
import os
from benchmark import benchmark

logger = logging.getLogger(__name__)


def main():
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', "--debug", action="store_true", help="Enable Debugging")
    parser.add_argument('-c', '--conf', required=True, help="JSON Config File")
    parser.add_argument('-pp', '--project-path', required=False, help="Project Path")
    parser.add_argument('-p', '--params', required=False, type=json.loads, help="JSON parameters")  # JSON parser
    args = parser.parse_args()

    # configure logging
    logging.basicConfig(format="%(asctime)s [%(name)s:%(lineno)d][%(funcName)s][%(levelname)s] %(message)s")

    # enable debugging
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logging.getLogger('benchmark').setLevel(logging.DEBUG)

    # open config and parse
    with open(args.conf) as data_file:
        conf = json.load(data_file)

    # check for project_path
    if args.project_path is None:
        if 'project_path' not in conf:
            raise Exception("missing 'project_path' in conf or command line")
        else:
            project_path = conf['project_path']
    else:
        project_path = args.project_path

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
        files = [project_path + "/" + p for p in conf['pyfiles']]
        pyfiles = "--py-files %s" % ",".join(files)

    # build spark-submit command
    cmd_raw = "{} spark-submit {} {} {}/{}".format(envs, params, pyfiles, project_path, conf['script'])

    # iterate paramters
    for r in args.params:
        cmd = cmd_raw.format(**r)  # add parameter

        # run shell command within benchmark
        with benchmark(cmd):
            logger.info(cmd)
            os.system(cmd)


if __name__ == "__main__":
    main()
