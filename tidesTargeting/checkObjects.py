"""Check the ZTF Objects using Lasair
Usage:
    checkObjects.py [--input=<PATH>] [--output=<PATH>] [--selection=<PATH>] [--name=<NAME>] [--key=<KEY>] [--chunk=<NUM>] [--plot=<BOOL>]
    checkObjects.py -h | --help | --version

Options:
    -i PATH, --input=PATH      The full path to the list containing the ZTF objects.
    -o PATH, --output=PATH     The full path to the output directory.
    -s PATH, --selection=PATH  The full path to the YAML file containing the selection function criteria.
    -n NAME, --name=NAME       The name of the selection function in the YAML file.
    -k KEY, --key=KEY          Your Lasair access token key or path to the YAML file containing it.
    -c NUM, --chunk=NUM        The number of objects to query at once. The Lasair API has a max of 50 objects per call. [default: 50]
    -p BOOL, --plot=BOOL       Do you want to save light curve plots in in the output directory? [default: True]
    -h --help                  Show this screen
    --version                  Show version

#Example:
#    python checkObjects.py -k 7a635469bb489bd4b70f1bc7bf3f39cffaemmm2a -s /data/chris/tidesSelectionFunctions.yml 
#    -n tidesSNZTFSelect -i /data/chris/ztfIAListDemo.dat -o /data/chris/dev_output/Demo/ -p T -c 50
#Note, the above key is jibberish. Please use your own Lasair token
"""

from docopt import docopt
import matplotlib
matplotlib.use('Agg')
import lasair
import numpy as np
import pandas as pd
import json
import yaml
import os, sys
from pathlib import Path
import matplotlib.pyplot as plt
import pprint

args = docopt(__doc__, version="Lasair Check Objects v0.1") ## Using the docopt method of parsing args

input = args['--input']
output = args['--output']
key = args['--key']
plot = args['--plot']
selection = args['--selection']
name = args['--name']
try:
    chunk = int(args['--chunk'])
except ValueError as ve:
    print('The chunk must be an integer')
    sys.exit(1)

if None in [input, output, key, selection, name]:
    message = """
    You must input at least:
    --input=<PATH>
    --output=<PATH>
    --key=<KEY>
    --selection=<PATH>
    --name=<NAME>
    """
    print(message)
    print(__doc__)
    sys.exit(1)

Path(output).mkdir(parents=True, exist_ok=True)

## Here we check if the user added a YAML file containing their password
## or if they added the key straight into the commandline
apiInput = key
if os.path.isfile(apiInput):
    keyOpen = yaml.load(open(apiInput), Loader=yaml.SafeLoader)
    token = keyOpen['lasair']['token']
else:
    token = str(apiInput)


if plot in ['True', 't', 'T', True]:
    makePlot = True
    print('Plots will be saved in: ', str(output))
elif plot in ['False', 'f', 'F', False]:
    makePlot=False
    print('No plots will be made.')
else:
    print("I don't understand the plot argument. Please provide a True/False input")
    sys.exit(0)

##Open Input YAML File
inputCriteriaPath = selection
inputCriteriaOpen = yaml.load(open(inputCriteriaPath), Loader=yaml.SafeLoader)
inputCriteriaName = inputCriteriaOpen[str(name)]

print('These are your filter criteria: ', inputCriteriaName)

print('Chunk Size:', chunk)



##Open the test file for the ZTF Objects
sn = pd.read_csv(str(input),header=None, names=['ztfname'])
numZTFtest = len(sn['ztfname'])


def splitIntoChunks(inLst, n):
    """The Lasair API can only handle 50 light curves a time.
    This function will split the list up into chunks of n objects. 
    """
    for i in range(0, len(inLst), n):
        yield inLst[i:i + n]

print('You have requested light curves for '+str(numZTFtest)+' objects from ZTF')
ztfNames = np.unique(sn['ztfname'])
#print(ztfNames)

#I've hardcoded 50 into the chunksize to comply with the LAsair API query. 
chunSize = chunk
if chunk>50:
    print('Max chunk size is 50.')
    print('')
    chunSize=50
ztfNameChunks = list(splitIntoChunks(ztfNames, chunSize))

print('Number of Chunks: ', len(ztfNameChunks))

# This is hardcoded for ZTF, will need to be changed for LSST as more filters are added.
filterDict = {1:'g', 2:'r', 3:'i', 'g':1, 'r':2, 'i':3}

##Lasair Acess Token
L = lasair.lasair_client(token)

#print(c)


def lightcurveSatify(criteria,lightcurve):
    '''
    Our paper states that the tides slection criteria is as follows:
    - only consider griz
    - must have at least 3 5sigma detections
    - Must have 5sigma detections across 2 nights
    - Must reach brighter than 22.5mag
    '''
    needFilters = criteria['filters']
    needSignificance = criteria['significance']
    minBands = criteria['minBands']
    minNights = criteria['minNights']
    magLimit = criteria['magLimit']
    
    filtersBool = np.array([filterDict[x] in inputCriteriaName['filters'] for x in lightcurve['fid']])
    significanceBool = 1.09/lightcurve['sigmapsf'] >= needSignificance
    
    sigAndFilterBool = filtersBool & significanceBool
    
    meetMinBands = len(np.unique(lightcurve['fid'][sigAndFilterBool])) >= minBands
    
    meetMinNight = len(np.unique(lightcurve['nid'][sigAndFilterBool])) >= minNights
    
    meetMagLimit = min(lightcurve['magpsf']) <= magLimit
    
    if meetMinBands == meetMinNight == meetMagLimit == True:
        return True
    else:
        return False

def plotLightCurve(name, lc, triggerDate=None, saveName=None):
    gIDX = lc['fid']==1
    rIDX = lc['fid']==2
    iIDX = lc['fid']==3
    nonDets = lc['candid'].isna()
    sigSatisfy = 1.09/lc['sigmapsf'] >= inputCriteriaName['significance']
    if 'diffmaglim' in lc.columns:
        plt.scatter(lc['jd'][nonDets & gIDX], lc['diffmaglim'][nonDets & gIDX], marker='v', color='green', alpha=0.5)
        plt.scatter(lc['jd'][nonDets & rIDX], lc['diffmaglim'][nonDets & rIDX], marker='v', color='red', alpha=0.5)
        plt.scatter(lc['jd'][nonDets & iIDX], lc['diffmaglim'][nonDets & iIDX], marker='v', color='yellow', alpha=0.5)

    plt.errorbar(lc['jd'][gIDX & sigSatisfy], lc['magpsf'][gIDX & sigSatisfy], yerr=lc['sigmapsf'][gIDX & sigSatisfy], fmt='o', color='green', label='g')
    plt.errorbar(lc['jd'][rIDX & sigSatisfy], lc['magpsf'][rIDX & sigSatisfy], yerr=lc['sigmapsf'][rIDX & sigSatisfy], fmt='o', color='red', label='r')
    plt.errorbar(lc['jd'][iIDX & sigSatisfy], lc['magpsf'][iIDX & sigSatisfy], yerr=lc['sigmapsf'][iIDX & sigSatisfy], fmt='o', color='yellow', label='i')

    if sum(gIDX & ~sigSatisfy & ~nonDets)!=0:
        plt.errorbar(lc['jd'][gIDX & ~sigSatisfy & ~nonDets], lc['magpsf'][gIDX & ~sigSatisfy & ~nonDets], yerr=lc['sigmapsf'][gIDX & ~sigSatisfy & ~nonDets], color='green', label='g < SNR',
    marker='x', linestyle='')
    if sum(rIDX & ~sigSatisfy& ~nonDets)!=0:
        plt.errorbar(lc['jd'][rIDX & ~sigSatisfy & ~nonDets], lc['magpsf'][rIDX & ~sigSatisfy & ~nonDets], yerr=lc['sigmapsf'][rIDX & ~sigSatisfy & ~nonDets], color='red', label='r < SNR',
    marker='x', linestyle='')
    if sum(iIDX & ~sigSatisfy& ~nonDets)!=0:
        plt.errorbar(lc['jd'][iIDX & ~sigSatisfy & ~nonDets], lc['magpsf'][iIDX & ~sigSatisfy & ~nonDets], yerr=lc['sigmapsf'][iIDX & ~sigSatisfy & ~nonDets], color='yellow', label='i < SNR',
    marker='x', linestyle='')
    
    if triggerDate!=-9999:
        plt.axvline(triggerDate, ls='--', c='k', label='Trigger Date')
    plt.gca().invert_yaxis()
    plt.title(name)
    plt.legend(loc='upper right')
    plt.xlabel('JD')
    plt.ylabel('Mag')
    
    if saveName is not None:
        plt.savefig(saveName, dpi=300, bbox_inches='tight')

totalListPassFail = []
totalDateTrigger = []
for z in range(len(ztfNameChunks)):
    passFail = []
    trigD = []
    ztfLoopIn = ztfNameChunks[z]
    c = L.lightcurves(ztfLoopIn)
    for i in range(len(c)): #Change the range to len(c)
        ztfN = ztfLoopIn[i]        
        print(ztfN)
        if len(c[i])==0:
            passFail.append('No Data')
            trigD.append(-9999)
            continue
        lc = pd.json_normalize(c[i]['candidates'])
                

        nonDets = lc['candid'].isna()

        
        ##Does the whole object Pass/Fail our cuts
        wholePF = lightcurveSatify(inputCriteriaName, lc)
        passFail.append(wholePF)
        if wholePF == True:
            ## Now we step through all the detections to test _when_ the object passed
            datesTest = np.unique(lc['jd'][~nonDets])
            pf = np.array(list((map(lambda x: lightcurveSatify(inputCriteriaName, lc[lc['jd']<=x]), datesTest))))
            dateItPasses = min(datesTest[pf])
        else: 
            dateItPasses = -9999
        if makePlot:
            plotLightCurve(str(ztfN),lc,triggerDate=dateItPasses, saveName=output+str(ztfN)+'.png')
            #plt.savefig(output+str(ztfN)+'_LightcurveCheck.png', bbox_inches='tight')
            plt.close()
        trigD.append(dateItPasses)
    totalListPassFail.append(passFail)
    totalDateTrigger.append(trigD)


listofObjectsPF = pd.DataFrame(np.column_stack((ztfNames,np.concatenate(totalListPassFail),np.concatenate(totalDateTrigger))), columns=['ZTFName', 'PassCut', 'TriggerDate'])
listofObjectsPF.to_csv(output+'PassFailCut.csv', index=False)