from prefect import flow, task
from prefect_dask import DaskTaskRunner
from lasair import lasair_consumer
import lasair
import json
import yaml
from random import randrange
import pandas as pd
import numpy as np
import sys

# This is hardcoded for ZTF, will need to be changed for LSST as more filters are added.
filterDict = {1:'g', 2:'r', 3:'i', 'g':1, 'r':2, 'i':3}



@task 
def loadTopicSettings(key):
  '''
  '''
  settingsOpen = yaml.load(open('./flowSettings.yaml'), Loader=yaml.SafeLoader)
  topic = settingsOpen[key]['topic']
  groupID = settingsOpen[key]['groupID']
  return topic, groupID


def loadLasairDetails(key):
  '''
  '''
  settingsOpen = yaml.load(open('./flowSettings.yaml'), Loader=yaml.SafeLoader)
  lasairToken = settingsOpen[key]['lasairToken']
  return lasairToken

def loadSelectionFunctionDetails(key):
  '''
  '''
  settingsOpen = yaml.load(open('./flowSettings.yaml'), Loader=yaml.SafeLoader)
  functionPath = settingsOpen[key]['selectFunctionPath']
  functionName = settingsOpen[key]['selectFunction']
  return functionPath, functionName

@task
def getDevBatch():
  """
  The Lasair Kafka stream sometimes goes down or ZTF isn't operational.
  This isn't great for the development because it always happens at the worst time!
  
  In this funciton I just load up a text file and pretend its a datastream!
  """
  dataIn = pd.read_csv("../tidesTargeting/ztfIAListDemo.dat", names=['objectId'])
  return dataIn

@task
def getLatestBatch(consumer):
  '''
  This task will query the topic and download the data in one batch
  '''
  recentObjects = pd.DataFrame()
  
  while True:
    msg = consumer.poll(timeout=5) #The kafka poll will wait 5 seconds to hear back. If nothing is delivered the pipeline will end and only objects 
    if msg is None:
      print('no more transients')
      break
      
    if msg.error():
      print(str(msg.error()))
      break
    jmsg = json.loads(msg.value())
    recentObjects = pd.concat([recentObjects,pd.DataFrame(jmsg, columns=jmsg.keys(), index=[0])], ignore_index=True)
  #print(recentObjects)
  return recentObjects

@task
def splitIntoChunks(inLst, n):
    """The Lasair API can only handle 50 light curves a time.
    This function will split the list up into chunks of n objects. 
    """
    for i in range(0, len(inLst), n):
        yield inLst[i:i + n]

@task
def lightcurveSatify(criteria,lightcurve, ztfname):
    '''
    Our paper states that the tides slection criteria is as follows:
    - only consider griz
    - must have at least 3 5sigma detections
    - Must have 5sigma detections across 2 nights
    - Must reach brighter than 22.5mag
    '''
    #print(lightcurve.columns)
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
        return ztfname, True
    else:
        return ztfname, False

@task
def daskCheckLightcurves(ztfName, c):
  if len(c)==0:
    return(False)
  lc = pd.json_normalize(c)
  print(lc)

  #nonDets = lc['candid'].isna()

  
  ##Does the whole object Pass/Fail our cuts
  wholePF = lightcurveSatify(inputCriteriaName, lc, ztfName)
  
  return(ztfName, wholePF)

@flow(task_runner=DaskTaskRunner())
def chunkyAssign(ztfNameChunks):
  namePassFail = []
  for chunk in ztfNameChunks:
    c = L.lightcurves(chunk)
    
    for idx in range(len(c)):
      #print(c[idx])
      if len(c[idx])==0:
        namePassFail.append([chunk[idx], False])
        continue
      else:
        lc = c[idx]['candidates']
        namePF = lightcurveSatify.submit(inputCriteriaName, pd.json_normalize(lc) ,chunk[idx])
        namePassFail.append(namePF)
  return namePassFail


@flow#(task_runner=DaskTaskRunner())
def checkChunksOfLightcurves(ztfLoopIn):
  passFail = []
  trigD = []
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
      doIwantToStepThrough = False
      if wholePF == True and doIwantToStepThrough == True:
          ## Now we step through all the detections to test _when_ the object passed
          datesTest = np.unique(lc['jd'][~nonDets])
          pf = np.array(list((map(lambda x: lightcurveSatify(inputCriteriaName, lc[lc['jd']<=x]), datesTest))))
          dateItPasses = min(datesTest[pf])
      else: 
          dateItPasses = -9999
      trigD.append(dateItPasses)
  return(passFail,trigD)
  
    # totalListPassFail.append(passFail)
    # totalDateTrigger.append(trigD)
    # return totalListPassFail, totalDateTrigger

@task
def passFailResultsDFandMerge(rPF, latestT):
  nPFdf = pd.DataFrame(rPF, columns=['Name','Pass'])
  merged = latestT.merge(nPFdf, left_on='objectId', right_on='Name', how='left')
  return merged

@flow
def executeCommPipe():
  my_topic, group_id =  loadTopicSettings('devConfig')

  print(my_topic, group_id)
  group_id = 'test{}'.format(randrange(1000)) ## Comment this out when doing pipeline for real
  print('Using group_id', group_id) #We'll fix our Group ID in production, but for now we randomise it so we have a good selection of objects. 


  consumer = lasair_consumer('kafka.lsst.ac.uk:9092', group_id, my_topic) ## Just accessing the Lasir interface to pull transients  

  latestTransients = getLatestBatch(consumer=consumer) ## (un)comment to use the real data stream.
  #print(latestTransients)
  #latestTransients = getDevBatch() ## Uncomment to use a test stream. i.e. Read a text file of objects
  if len(latestTransients) == 0:
    print('!!! No Transients !!!')
    return None
  print('All transients: ', len(latestTransients))
  ztfNames = np.unique(latestTransients['objectId'])
  print('Unique transients: ', len(ztfNames))


  ztfNameChunks = list(splitIntoChunks(ztfNames, 10))

  print(ztfNameChunks)
  #print(list(map(checkChunksOfLightcurves, ztfNameChunks)))

  nPF = chunkyAssign(ztfNameChunks)
  resultPassFail = [x.result() for x in nPF]
  
  mergedDF = passFailResultsDFandMerge(resultPassFail, latestTransients)
  print(mergedDF)


  

if __name__ == "__main__":
  lasairToken = loadLasairDetails('devConfig')
  L = lasair.lasair_client(lasairToken)

  inputCriteriaPath, selectFuncName =  loadSelectionFunctionDetails('devConfig')
  inputCriteriaOpen = yaml.load(open(inputCriteriaPath), Loader=yaml.SafeLoader)
  inputCriteriaName = inputCriteriaOpen[str(selectFuncName)]

  executeCommPipe()