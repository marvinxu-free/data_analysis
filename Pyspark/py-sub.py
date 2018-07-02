"""
"""
__author__ = "maxent"
__credits__ = ["xuchao"]
__status__ = "production"

import subprocess
import os
import argparse
import sys
import copy

sparkHome = ["/services/software/spark/spark-2.1.0-bin-2.3.0-cdh5.1.3/bin/spark-submit"]
sparkLocalHome = ["/usr/local/Cellar/apache-spark/2.1.1/libexec/bin/spark-submit"]


def runCommand(command, file):
    """
    """
    print file
    obs_dir, file_name = os.path.split(file)
    LOG_PATH = obs_dir + "/log"
    print LOG_PATH

    runLog = "{0}/{1}_run.log".format(LOG_PATH, file_name)
    errLog = "{0}/{1}_err.log".format(LOG_PATH, file_name)
    frun = open(runLog, "w")
    sys.stdout = frun
    ferr = open(errLog, "w")
    sys.stderr = ferr
    process = subprocess.Popen(command, stdout=frun, stderr=ferr, shell=False)
    returncode = process.poll()
    while returncode is None:
        returncode = process.poll()
    return returncode


def runDemo(args):
    """
    """
    master = ["--master", "yarn"]
    deployMode = ["--deploy-mode", "client"]
    hiveConf = ["--files", "/services/software/spark/spark-2.1.0-bin-2.3.0-cdh5.1.3/conf/hive-site.xml"]
    resources = ["--num-executors", "10", "--executor-memory", "1G", "--executor-cores", "12",
                 "--driver-memory", "1G",
                 "--conf", "spark.yarn.executor.memoryOverhead=2g", "--conf", "spark.driver.maxResultSize=1g"]
    command = sparkHome + master + deployMode + hiveConf + resources
    if args.zip_file is not None:
        abs_zip = os.path.abspath(args.zip_file)
        zip_cmd = ['--py-files'] + [abs_zip] + ["--files"] + [abs_zip]
        command += zip_cmd
    else:
        exit(1)

    if args.file is not None and args.ratio is not None:
        ratio_str = map(lambda x: str(x), args.ratio)
        print "ratio is {0}".format(ratio_str)
        abs_zip = os.path.abspath(args.zip_file)
        ratio_cmd = ['-r'] + ratio_str + ["-z", abs_zip]
        files = args.file
        for file in files:
            abs_file = os.path.abspath(file)
            argument_copy = copy.deepcopy(command)
            name_args = ["--name", "xuchao running {0}".format(os.path.basename(abs_file))]
            argument_copy.extend(name_args)
            argument_copy.extend([abs_file])
            argument_copy = argument_copy + ratio_cmd
            print argument_copy
            runCommand(argument_copy, abs_file)
    elif args.file is not None:
        files = args.file
        abs_zip = os.path.abspath(args.zip_file)
        for file in files:
            abs_file = os.path.abspath(file)
            argument_copy = copy.deepcopy(command)
            name_args = ["--name", "Xu Chao running {0}".format(os.path.basename(abs_file))]
            argument_copy.extend(name_args)
            argument_copy.extend([abs_file])
            argument_copy = argument_copy + ["-z", abs_zip]
            print argument_copy
            runCommand(argument_copy, abs_file)
    else:
        exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', action='store', default=None, dest='file', help="submit python file", nargs='+')
    parser.add_argument('-r', '--ratio', action='store', default=None, dest='ratio', help="maxent_id fraud ratio",
                        nargs='+')
    parser.add_argument('-z', '--zip_file', action='store', type=str, default=None, dest='zip_file',
                        help="dependence zip file")
    args = parser.parse_args()
    runDemo(args=args)
