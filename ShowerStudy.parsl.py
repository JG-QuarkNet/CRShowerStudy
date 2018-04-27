import argparse
import parsl
from parsl import *

workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(executors=[workers])
# Log to STDOUT
#parsl.set_stream_logger()

## Define Apps ##
@App('bash', dfk)
def WireDelay(threshIn='', outputs=[], geoDir='', daqId='', fw=''):
		return 'perl perl/WireDelay.pl %s %s %s %s %s' %(threshIn,outputs[0],geoDir,daqId,fw)

#@App('bash', dfk)
#def WireDelay(inputs=[], outputs=[], geoDir='', daqId='', fw=''):
#		return 'perl perl/WireDelay.pl %s %s %s %s %s' %(inputs[0],outputs[0],geoDir,daqId,fw)

@App('bash', dfk)
def Combine(inputs=[],outputs=[]):
		#filenames = [str(i) for i in inputs]
		print("inside Combine checkpoint 1")
		#print(' '.join(filenames) )
		#print('perl perl/Combine.pl ' + ' '.join(inputs) + ' ' + str(outputs[0]))
		#print("inside Combine checkpoint 2")
		return 'perl perl/Combine.pl ' + ' '.join(inputs) + ' ' + str(outputs[0])

@App('bash', dfk)
def Sort(inputs=[], outputs=[], key1='1', key2='1'):
		return 'perl perl/Sort.pl %s %s %s %s' %(inputs[0], outputs[0], key1, key2)

@App('bash', dfk)
def EventSearch(inputs=[], outputs=[], gate='', detCoinc='2', chanCoinc='2', eventCoinc='2'):
		return 'perl perl/EventSearch.pl %s %s %s %s %s %s' %(inputs[0],outputs[0],gate,detCoinc,chanCoinc,eventCoinc)


## Parse the command-line arguments ##
parser = argparse.ArgumentParser()

parser.add_argument("--thresholdAll", nargs='+', help="All threshold files")
parser.add_argument("--wireDelayData", nargs='+', help="Filenames for intermediate Wire Delay data")
parser.add_argument("--geoDir", help="Directory containing DAQ ID directories that contain .geo files")
#parser.add_argument("--geoFiles", nargs='+', help=".geo filenames for each CRD")
parser.add_argument("--detectors", nargs='+', help="IDs of all CRDs in the analysis")
parser.add_argument("--firmwares", nargs='+', help="DAQ firmware versions")
parser.add_argument("--combineOut", help="Combined data from all intermediate Wire Delay files")
parser.add_argument("--sort_sortKey1")
parser.add_argument("--sort_sortKey2")
parser.add_argument("--sortOut")
parser.add_argument("--gate")
parser.add_argument("--detectorCoincidence")
parser.add_argument("--channelCoincidence")
parser.add_argument("--eventCoincidence")
parser.add_argument("--eventCandidates", help="eventCandidates file")

args = parser.parse_args()


## The Workflow ##

# 1) WireDelay() takes input Threshold (.thresh) files and converts
#    each to a Wire Delay (.wd) file:
WireDelay_futures = []
for i in range(len(args.thresholdAll)):
		WireDelay_futures.append(WireDelay(threshIn=args.thresholdAll[i], outputs=[args.wireDelayData[i]], geoDir=args.geoDir, daqId=args.detectors[i],fw=args.firmwares[i]))

# WireDelay_futures is a list of futures.
# Each future has an outputs list with one output.
WireDelay_outputs = [i.outputs[0] for i in WireDelay_futures]

print("pre-combine checkpoint")

# 2) Combine() takes the WireDelay files output by WireDelay() and combines
#    them into a single file with name given by --combineOut
#print(WireDelay_outputs, [args.combineOut])
Combine_future = Combine(inputs=WireDelay_outputs, outputs=[args.combineOut])

# 3) Sort() sorts the --combineOut file, producing a new file with name given
#    by --sortOut
Sort_future = Sort(inputs=Combine_future.outputs, outputs=[args.sortOut], key1=args.sort_sortKey1, key2=args.sort_sortKey2)

# 4) EventSearch() processes the --sortOut file and identifies event
#    candidates in a output file with name given by --eventCandidates
# NB: This output file is interpreted by the e-Lab webapp, which expects it
#    to be named "eventCandidates"
EventSearch_future = EventSearch(inputs=Sort_future.outputs, outputs=[args.eventCandidates], gate=args.gate, detCoinc=args.detectorCoincidence, chanCoinc=args.channelCoincidence, eventCoinc=args.eventCoincidence)

# Wait for the final result before exiting.
x = EventSearch_future.result()

print("Call to EventSearch completed with exit code:", x)
