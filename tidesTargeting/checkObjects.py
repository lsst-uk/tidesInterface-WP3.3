import matplotlib
matplotlib.use('Agg')
import lasair
import numpy as np
import pandas as pd
import json
import yaml
import argparse
import os, sys
from pathlib import Path
import matplotlib.pyplot as plt

## Argparse stuff to read in the commandline arguments
parser = argparse.ArgumentParser()
parser.add_argument('-in', '--input', help="Full path to the list of ZTF SNe to evaluate")
parser.add_argument('-outdir', '--output', help="Full path to the output directory")
parser.add_argument('-s', '--selection', help="Full path to the selection function YAML file")
parser.add_argument('-n', '--name', help="Name of selection criteria within YAML file")
parser.add_argument('-k', '--key', help="Either your Lasair access token or the YAML file your token is stored in")
parser.add_argument('-c', '--chunk', nargs='?', default=50, type=int, help="The chunk size of your input target list. The Lasair API has a 50 object limit, set this number to be the maximum number of objects per API call. By default it is 50.")
parser.add_argument('-p', '--plot', default=True, help="Do you want to save light curve plots?")


# Execute the parse_args() method
args = parser.parse_args()

# Make the output directory
Path(args.output).mkdir(parents=True, exist_ok=True)

## Here we check if the user added a YAML file containing their password
## or if they added the key straight into the commandline
apiInput = args.key
if os.path.isfile(apiInput):
    keyOpen = yaml.load(open(apiInput), Loader=yaml.SafeLoader)
    token = keyOpen['lasair']['token']
else:
    token = str(apiInput)

#print(token)

## Plots?

if args.plot in ['True', 't', 'T', True]:
    makePlot = True
    print('Plots will be saved in: ', str(args.output))
elif args.plot in ['False', 'f', 'F', False]:
    makePlot=False
    print('No plots will be made.')
else:
    print("I don't understand the plot argument. Please provide a True/False input")
    sys.exit(0)

##Open Input YAML File
inputCriteriaPath = args.selection
inputCriteriaOpen = yaml.load(open(inputCriteriaPath), Loader=yaml.SafeLoader)
inputCriteriaName = inputCriteriaOpen[str(args.name)]

print('These are your filter criteria: ', inputCriteriaName)

print('Chunk Size:', args.chunk)



##Open the test file for the ZTF Objects
sn = pd.read_csv(str(args.input),header=None, names=['ztfname'])
numZTFtest = len(sn['ztfname'])

# We don't need this check because I have now implemented a script to chuknk up the input data
# if numZTFtest>1000:
#     print('Please request fewer than 1,000 objects. This is a Lasair query restriction')
#     sys.exit()

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
chunSize = args.chunk
if args.chunk>50:
    print('Max chunk size is 50.')
    print('')
    chunSize=50
ztfNameChunks = list(splitIntoChunks(ztfNames, chunSize))

print('Number of Chunks: ', len(ztfNameChunks))

# This is hardcoded for ZTF, will need to be changed for LSST as more filters are added.
filterDict = {1:'g', 2:'r', 'g':1, 'r':2}

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
    nonDets = lc['candid'].isna()
    sigSatisfy = 1.09/lc['sigmapsf'] >= inputCriteriaName['significance']
    if 'diffmaglim' in lc.columns:
        plt.scatter(lc['jd'][nonDets & gIDX], lc['diffmaglim'][nonDets & gIDX], marker='v', color='green', alpha=0.5)
        plt.scatter(lc['jd'][nonDets & rIDX], lc['diffmaglim'][nonDets & rIDX], marker='v', color='red', alpha=0.5)

    plt.errorbar(lc['jd'][gIDX & sigSatisfy], lc['magpsf'][gIDX & sigSatisfy], yerr=lc['sigmapsf'][gIDX & sigSatisfy], fmt='o', color='green', label='g')
    plt.errorbar(lc['jd'][rIDX & sigSatisfy], lc['magpsf'][rIDX & sigSatisfy], yerr=lc['sigmapsf'][rIDX & sigSatisfy], fmt='o', color='red', label='r')

    if sum(gIDX & ~sigSatisfy & ~nonDets)!=0:
        plt.errorbar(lc['jd'][gIDX & ~sigSatisfy & ~nonDets], lc['magpsf'][gIDX & ~sigSatisfy & ~nonDets], yerr=lc['sigmapsf'][gIDX & ~sigSatisfy & ~nonDets], color='green', label='g < SNR',
    marker='x')
    if sum(rIDX & ~sigSatisfy& ~nonDets)!=0:
        plt.errorbar(lc['jd'][rIDX & ~sigSatisfy & ~nonDets], lc['magpsf'][rIDX & ~sigSatisfy & ~nonDets], yerr=lc['sigmapsf'][rIDX & ~sigSatisfy & ~nonDets], color='red', label='r < SNR',
    marker='x')
    
    if triggerDate is not None:
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
        lc = pd.json_normalize(c[i])
        #print(lc)

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
            plotLightCurve(str(ztfN),lc,triggerDate=dateItPasses, saveName=args.output+str(ztfN)+'.png')
            #plt.savefig(args.output+str(ztfN)+'_LightcurveCheck.png', bbox_inches='tight')
            plt.close()
        trigD.append(dateItPasses)
    totalListPassFail.append(passFail)
    totalDateTrigger.append(trigD)


listofObjectsPF = pd.DataFrame(np.column_stack((ztfNames,np.concatenate(totalListPassFail),np.concatenate(totalDateTrigger))), columns=['ZTFName', 'PassCut', 'TriggerDate'])
listofObjectsPF.to_csv(args.output+'PassFailCut.csv', index=False)