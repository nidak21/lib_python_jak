
import sys
import argparse
#-----------------------
def process():
    args = parseCmdLine()
    print args
    print args.verbose

#-----------------------
def parseCmdLine():
    """
    Usage: scriptname [-v] [ randForSplit [ randForClassifier] ]
    """
    parser = argparse.ArgumentParser( \
    description='Run a tuning experiment, log to tuning.log')

    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true',
			help='verbose: print longer tuning report')

    parser.add_argument('randForSplit', nargs='?', default=None, type=int,
			help="random seed for test_train_split")

    parser.add_argument('randForClassifier', nargs='?', default=None, type=int,
			help="random seed for classifier")
    return parser.parse_args()
#-----------------------
if __name__ == "__main__": process()
