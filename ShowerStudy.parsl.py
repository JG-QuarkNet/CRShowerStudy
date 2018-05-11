import parsl
from parsl import *

local_config = {
    "sites" : [
        { "site" : "Threads",
          "auth" : { "channel" : None },
          "execution" : {
              "executor" : "threads",
              "provider" : None,
              "max_workers" : 4
          }
        }],
    "globals" : {"lazyErrors" : True}
}

dfk = DataFlowKernel(config=local_config)


## Define Apps ##
@App('bash', dfk)
def WireDelay(threshIn='', outputs=[], geoDir='', daqId='', fw=''):
        return 'perl ./perl/WireDelay.pl %s %s %s %s %s' %(threshIn,outputs[0],geoDir,daqId,fw)

@App('bash', dfk)
def Combine(inputs=[],outputs=[]):
        return 'perl ./perl/Combine.pl ' + ' '.join(inputs) + ' ' + str(outputs[0])

@App('bash', dfk)
def Sort(inputs=[], outputs=[], key1='1', key2='1'):
        return 'perl ./perl/Sort.pl %s %s %s %s' %(inputs[0], outputs[0], key1, key2)

@App('bash', dfk)
def EventSearch(inputs=[], outputs=[], gate='', detCoinc='2', chanCoinc='2', eventCoinc='2'):
        return 'perl ./perl/EventSearch.pl %s %s %s %s %s %s' %(inputs[0],outputs[0],gate,detCoinc,chanCoinc,eventCoinc)


## Analysis Parameters ##
# Define what are typically the command-line arguments
thresholdAll = ('files/6119.2016.0104.1.thresh', 'files/6203.2016.0104.1.thresh')
wireDelayData = ('6119.2016.0104.1.wd', '6203.2016.0104.1.wd')
geoDir = './geo'
detectors = ('6119', '6203')
firmwares = ('1.12', '1.12')
combineOut = 'combinedData'
sort_sortKey1 = '2'
sort_sortKey2 = '3'
sortOut = 'sortedData'
gate = '1000'
detectorCoincidence = '2'
channelCoincidence = '2'
eventCoincidence = '2'
eventCandidates = 'eventCandidates'


## Workflow ##
# 1) WireDelay() takes input Threshold (.thresh) files and converts
#    each to a Wire Delay (.wd) file:
WireDelay_futures = []
for i in range(len(thresholdAll)):
        WireDelay_futures.append(WireDelay(threshIn=thresholdAll[i], outputs=[wireDelayData[i]], geoDir=geoDir, daqId=detectors[i],fw=firmwares[i]))

# WireDelay_futures is a list of futures.
# Each future has an outputs list with one output.
WireDelay_outputs = [i.outputs[0] for i in WireDelay_futures]

# 2) Combine() takes the WireDelay files output by WireDelay() and combines
#    them into a single file with name given by --combineOut
Combine_future = Combine(inputs=WireDelay_outputs, outputs=[combineOut])

# 3) Sort() sorts the --combineOut file, producing a new file with name given
#    by --sortOut
Sort_future = Sort(inputs=Combine_future.outputs, outputs=[sortOut], key1=sort_sortKey1, key2=sort_sortKey2)

# 4) EventSearch() processes the --sortOut file and identifies event
#    candidates in a output file with name given by --eventCandidates
# NB: This output file is interpreted by the e-Lab webapp, which expects it
#    to be named "eventCandidates"
EventSearch_future = EventSearch(inputs=Sort_future.outputs, outputs=[eventCandidates], gate=gate, detCoinc=detectorCoincidence, chanCoinc=channelCoincidence, eventCoinc=eventCoincidence)

# Wait for the final result before exiting.
x = EventSearch_future.result()

print("Call to EventSearch completed with exit code:", x)
