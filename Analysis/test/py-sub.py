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
    LOG_PATH = os.path.dirname(os.path.abspath(file)) + "/log"
    print LOG_PATH

    runLog = "{0}/run.log".format(LOG_PATH)
    errLog = "{0}/err.log".format(LOG_PATH)
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

    ratio_str = map(lambda x: str(x),args.ratio)
    # ratios = args.ratio
    print "ratio is {0}".format(ratio_str)
    ratio_cmd = ['-r'] + ratio_str
    argument = []
    if args.file is not None:
        files = args.file
        for file in files:
            abs_file = os.path.abspath(file)
            argument_copy = copy.deepcopy(argument)
            argument_copy.extend([abs_file])
            command = sparkHome +  master + deployMode + hiveConf + resources + argument_copy + ratio_cmd
            print command
            runCommand(command, file)
    else:
        exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-f','--file',action='store',default=None,dest='file',help="submit python file",nargs='+')
    parser.add_argument('-r','--ratio',action='store',default=0.05,dest='ratio',help="maxent_id fraud ratio",nargs='+')
    args = parser.parse_args()
    runDemo(args=args)
