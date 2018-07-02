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
    obs_dir, file_name = os.path.splitext(file)
    LOG_PATH = os.path.dirname(obs_dir) + "/log"
    print LOG_PATH


    runLog = "{0}/{1}_run.log".format(LOG_PATH, file_name)
    errLog = "{0}/{1}_err.log".format(LOG_PATH, file_name)
    frun = open(runLog,"w")
    sys.stdout =frun
    ferr = open(errLog,"w")
    sys.stderr =ferr
    process = subprocess.Popen(command, stdout=frun,stderr=ferr,shell=False)
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
    resources = ["--num-executors", "8", "--executor-memory", "8G", "--executor-cores", "8", "--driver-memory", "2G"]

    if args.file is not None and args.ratio is not None:
        ratio_str = map(lambda x: str(x),args.ratio)
        print "ratio is {0}".format(ratio_str)
        ratio_cmd = ['-r'] + ratio_str
        files = args.file
        for file in files:
            abs_file = os.path.abspath(file)
            argument_copy = []
            argument_copy.extend([abs_file])
            command = sparkHome +  master + deployMode + hiveConf + resources + argument_copy + ratio_cmd
            print command
            runCommand(command, file)
    elif args.file is not None:
        files = args.file
        for file in files:
            abs_file = os.path.abspath(file)
            argument_copy = []
            argument_copy.extend([abs_file])
            command = sparkHome + master + deployMode + hiveConf + resources + argument_copy
            print command
            runCommand(command, file)
    else:
        exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-f','--file',action='store',default=None,dest='file',help="submit python file",nargs='+')
    parser.add_argument('-r','--ratio',action='store',default=None,dest='ratio',help="maxent_id fraud ratio",nargs='+')
    args = parser.parse_args()
    runDemo(args=args)
