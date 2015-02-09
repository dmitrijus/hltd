# /users/avetisya/LS1/DAQTest/HLT/V3 (CMSSW_7_2_1)

import FWCore.ParameterSet.Config as cms

process = cms.Process( "HLT" )

process.HLTConfigVersion = cms.PSet(
  tableName = cms.string('/users/avetisya/LS1/DAQTest/HLT/V3')
)

process.streams = cms.PSet( 
  A = cms.vstring( 'A1' ),
  B = cms.vstring( 'B' ),
  DQM = cms.vstring( 'DQM1' )
)
process.datasets = cms.PSet( 
  A1 = cms.vstring( 'p1' ),
  B = cms.vstring( 'p3' ),
  DQM1 = cms.vstring( 'p2' )
)

process.source = cms.Source( "FedRawDataInputSource",
    numBuffers = cms.untracked.uint32( 1 ),
    useL1EventID = cms.untracked.bool( True ),
    eventChunkSize = cms.untracked.uint32( 128 ),
    eventChunkBlock = cms.untracked.uint32( 128 ),
    getLSFromFilename = cms.untracked.bool( True ),
    verifyAdler32 = cms.untracked.bool( True )
)

process.PoolDBESSource = cms.ESSource( "PoolDBESSource",
    globaltag = cms.string( "GR_H_V39::All" ),
    RefreshEachRun = cms.untracked.bool( False ),
    RefreshOpenIOVs = cms.untracked.bool( False ),
    toGet = cms.VPSet( 
    ),
    DBParameters = cms.PSet( 
      authenticationPath = cms.untracked.string( "." ),
      connectionRetrialTimeOut = cms.untracked.int32( 60 ),
      idleConnectionCleanupPeriod = cms.untracked.int32( 10 ),
      messageLevel = cms.untracked.int32( 0 ),
      enablePoolAutomaticCleanUp = cms.untracked.bool( False ),
      enableConnectionSharing = cms.untracked.bool( True ),
      enableReadOnlySessionOnUpdateConnection = cms.untracked.bool( False ),
      connectionTimeOut = cms.untracked.int32( 0 ),
      authenticationSystem = cms.untracked.int32( 0 ),
      connectionRetrialPeriod = cms.untracked.int32( 10 )
    ),
    RefreshAlways = cms.untracked.bool( False ),
    connect = cms.string( "frontier://(proxyurl=http://localhost:3128)(serverurl=http://localhost:8000/FrontierOnProd)(serverurl=http://localhost:8000/FrontierOnProd)(retrieve-ziplevel=0)/CMS_COND_31X_GLOBALTAG" ),
    ReconnectEachRun = cms.untracked.bool( False ),
    BlobStreamerName = cms.untracked.string( "TBufferBlobStreamingService" )
)

process.FastTimerService = cms.Service( "FastTimerService",
    dqmPath = cms.untracked.string( "HLT/TimerService" ),
    dqmModuleTimeRange = cms.untracked.double( 40.0 ),
    useRealTimeClock = cms.untracked.bool( True ),
    enableTimingModules = cms.untracked.bool( True ),
    enableDQM = cms.untracked.bool( True ),
    enableDQMbyModule = cms.untracked.bool( False ),
    enableTimingExclusive = cms.untracked.bool( False ),
    skipFirstPath = cms.untracked.bool( False ),
    enableDQMbyLumiSection = cms.untracked.bool( True ),
    dqmPathTimeResolution = cms.untracked.double( 0.5 ),
    dqmPathTimeRange = cms.untracked.double( 100.0 ),
    dqmTimeRange = cms.untracked.double( 1000.0 ),
    dqmLumiSectionsRange = cms.untracked.uint32( 2500 ),
    enableDQMbyProcesses = cms.untracked.bool( True ),
    enableDQMSummary = cms.untracked.bool( True ),
    enableTimingSummary = cms.untracked.bool( False ),
    enableDQMbyPathTotal = cms.untracked.bool( True ),
    enableTimingPaths = cms.untracked.bool( True ),
    enableDQMbyPathExclusive = cms.untracked.bool( True ),
    dqmTimeResolution = cms.untracked.double( 5.0 ),
    dqmModuleTimeResolution = cms.untracked.double( 0.2 ),
    enableDQMbyPathActive = cms.untracked.bool( True ),
    enableDQMbyPathDetails = cms.untracked.bool( True ),
    enableDQMbyPathOverhead = cms.untracked.bool( True ),
    enableDQMbyPathCounters = cms.untracked.bool( True ),
    enableDQMbyModuleType = cms.untracked.bool( False )
)
process.DQMStore = cms.Service( "DQMStore",
    verbose = cms.untracked.int32( 0 ),
    collateHistograms = cms.untracked.bool( False ),
    enableMultiThread = cms.untracked.bool( True ),
    forceResetOnBeginLumi = cms.untracked.bool( False ),
    LSbasedMode = cms.untracked.bool( True ),
    verboseQT = cms.untracked.int32( 0 )
)
process.EvFDaqDirector = cms.Service( "EvFDaqDirector",
    buBaseDir = cms.untracked.string( "." ),
    runNumber = cms.untracked.uint32( 0 ),
    outputAdler32Recheck = cms.untracked.bool( False ),
    baseDir = cms.untracked.string( "." )
)
process.FastMonitoringService = cms.Service( "FastMonitoringService",
    slowName = cms.untracked.string( "slowmoni" ),
    sleepTime = cms.untracked.int32( 1 ),
    fastMonIntervals = cms.untracked.uint32( 2 ),
    fastName = cms.untracked.string( "fastmoni" )
)
process.PrescaleService = cms.Service( "PrescaleService",
    forceDefault = cms.bool( False ),
    prescaleTable = cms.VPSet( 
      cms.PSet(  pathName = cms.string( "p3" ),
        prescales = cms.vuint32( 50, 50, 50, 50, 50, 50, 50, 50, 50 )
      ),
      cms.PSet(  pathName = cms.string( "p2" ),
        prescales = cms.vuint32( 100, 100, 100, 100, 100, 100, 100, 100, 100 )
      ),
      cms.PSet(  pathName = cms.string( "p1" ),
        prescales = cms.vuint32( 10, 10, 10, 10, 10, 10, 10, 10, 10 )
      )
    ),
    lvl1DefaultLabel = cms.string( "1e33" ),
    lvl1Labels = cms.vstring( '2e33',
      '1.4e33',
      '1e33',
      '7e32',
      '5e32',
      '3e32',
      '2e32',
      '1.4e32',
      '1e32' )
)
process.MessageLogger = cms.Service( "MessageLogger",
    suppressInfo = cms.untracked.vstring( 'hltGtDigis' ),
    debugs = cms.untracked.PSet( 
      threshold = cms.untracked.string( "INFO" ),
      placeholder = cms.untracked.bool( True ),
    ),
    cout = cms.untracked.PSet( 
      threshold = cms.untracked.string( "ERROR" ),
    ),
    cerr_stats = cms.untracked.PSet( 
      threshold = cms.untracked.string( "WARNING" ),
      output = cms.untracked.string( "cerr" ),
      optionalPSet = cms.untracked.bool( True )
    ),
    warnings = cms.untracked.PSet( 
      threshold = cms.untracked.string( "INFO" ),
      placeholder = cms.untracked.bool( True ),
    ),
    statistics = cms.untracked.vstring( 'cerr' ),
    cerr = cms.untracked.PSet( 
      INFO = cms.untracked.PSet(  limit = cms.untracked.int32( 0 ) ),
      noTimeStamps = cms.untracked.bool( False ),
      FwkReport = cms.untracked.PSet( 
        reportEvery = cms.untracked.int32( 1 ),
        limit = cms.untracked.int32( 0 )
      ),
      default = cms.untracked.PSet(  limit = cms.untracked.int32( 10000000 ) ),
      Root_NoDictionary = cms.untracked.PSet(  limit = cms.untracked.int32( 0 ) ),
      FwkJob = cms.untracked.PSet(  limit = cms.untracked.int32( 0 ) ),
      FwkSummary = cms.untracked.PSet( 
        reportEvery = cms.untracked.int32( 1 ),
        limit = cms.untracked.int32( 10000000 )
      ),
      threshold = cms.untracked.string( "INFO" ),
    ),
    FrameworkJobReport = cms.untracked.PSet( 
      default = cms.untracked.PSet(  limit = cms.untracked.int32( 0 ) ),
      FwkJob = cms.untracked.PSet(  limit = cms.untracked.int32( 10000000 ) )
    ),
    suppressWarning = cms.untracked.vstring( 'hltGtDigis' ),
    errors = cms.untracked.PSet( 
      threshold = cms.untracked.string( "INFO" ),
      placeholder = cms.untracked.bool( True ),
    ),
    fwkJobReports = cms.untracked.vstring( 'FrameworkJobReport' ),
    infos = cms.untracked.PSet( 
      threshold = cms.untracked.string( "INFO" ),
      Root_NoDictionary = cms.untracked.PSet(  limit = cms.untracked.int32( 0 ) ),
      placeholder = cms.untracked.bool( True ),
    ),
    categories = cms.untracked.vstring( 'FwkJob',
      'FwkReport',
      'FwkSummary',
      'Root_NoDictionary' ),
    destinations = cms.untracked.vstring( 'warnings',
      'errors',
      'infos',
      'debugs',
      'cout',
      'cerr' ),
    threshold = cms.untracked.string( "INFO" ),
    suppressError = cms.untracked.vstring( 'hltGtDigis' )
)

process.ExceptionGenerator2 = cms.EDAnalyzer( "ExceptionGenerator",
    defaultAction = cms.untracked.int32( 0 ),
    defaultQualifier = cms.untracked.int32( 0 )
)
process.HLTPrescaler = cms.EDFilter( "HLTPrescaler",
    L1GtReadoutRecordTag = cms.InputTag( "hltGtDigis" ),
    offset = cms.uint32( 0 )
)
process.HLTPrescaler2 = cms.EDFilter( "HLTPrescaler",
    L1GtReadoutRecordTag = cms.InputTag( "hltGtDigis" ),
    offset = cms.uint32( 0 )
)
process.hltL1GtObjectMap = cms.EDProducer( "L1GlobalTrigger",
    TechnicalTriggersUnprescaled = cms.bool( True ),
    ProduceL1GtObjectMapRecord = cms.bool( True ),
    AlgorithmTriggersUnmasked = cms.bool( False ),
    EmulateBxInEvent = cms.int32( 1 ),
    AlgorithmTriggersUnprescaled = cms.bool( True ),
    ProduceL1GtDaqRecord = cms.bool( False ),
    ReadTechnicalTriggerRecords = cms.bool( True ),
    RecordLength = cms.vint32( 3, 0 ),
    TechnicalTriggersUnmasked = cms.bool( False ),
    ProduceL1GtEvmRecord = cms.bool( False ),
    GmtInputTag = cms.InputTag( "hltGtDigis" ),
    TechnicalTriggersVetoUnmasked = cms.bool( True ),
    AlternativeNrBxBoardEvm = cms.uint32( 0 ),
    TechnicalTriggersInputTags = cms.VInputTag( 'simBscDigis' ),
    CastorInputTag = cms.InputTag( "castorL1Digis" ),
    GctInputTag = cms.InputTag( "hltGctDigis" ),
    AlternativeNrBxBoardDaq = cms.uint32( 0 ),
    WritePsbL1GtDaqRecord = cms.bool( False ),
    BstLengthBytes = cms.int32( -1 )
)
process.TriggerJSONMonitoring = cms.EDAnalyzer( "TriggerJSONMonitoring",
    triggerResults = cms.InputTag( 'TriggerResults','','HLT' )
)
process.DQMFileSaver = cms.EDAnalyzer( "DQMFileSaver",
    runIsComplete = cms.untracked.bool( False ),
    referenceHandling = cms.untracked.string( "all" ),
    producer = cms.untracked.string( "DQM" ),
    forceRunNumber = cms.untracked.int32( -1 ),
    saveByRun = cms.untracked.int32( 1 ),
    saveAtJobEnd = cms.untracked.bool( False ),
    saveByLumiSection = cms.untracked.int32( 1 ),
    version = cms.untracked.int32( 1 ),
    referenceRequireStatus = cms.untracked.int32( 100 ),
    convention = cms.untracked.string( "FilterUnit" ),
    dirName = cms.untracked.string( "." ),
    fileFormat = cms.untracked.string( "PB" )
)
process.ExceptionGenerator = cms.EDAnalyzer( "ExceptionGenerator",
    defaultAction = cms.untracked.int32( 0 ),
    defaultQualifier = cms.untracked.int32( 64 )
)
process.ExceptionGenerator3 = cms.EDAnalyzer( "ExceptionGenerator",
    defaultAction = cms.untracked.int32( 0 ),
    defaultQualifier = cms.untracked.int32( 0 )
)
process.HLTPrescaler3 = cms.EDFilter( "HLTPrescaler",
    L1GtReadoutRecordTag = cms.InputTag( "hltGtDigis" ),
    offset = cms.uint32( 0 )
)

process.hltOutputA = cms.OutputModule( "ShmStreamConsumer",
    SelectEvents = cms.untracked.PSet(  SelectEvents = cms.vstring( 'p1' ) ),
    outputCommands = cms.untracked.vstring( 'drop *',
      'keep FEDRawDataCollection_rawDataCollector_*_*',
      'keep FEDRawDataCollection_source_*_*' )
)
process.hltOutputB = cms.OutputModule( "ShmStreamConsumer",
    SelectEvents = cms.untracked.PSet(  SelectEvents = cms.vstring( 'p3' ) ),
    outputCommands = cms.untracked.vstring( 'drop *',
      'keep FEDRawDataCollection_rawDataCollector_*_*',
      'keep FEDRawDataCollection_source_*_*' )
)
process.hltOutputDQM = cms.OutputModule( "ShmStreamConsumer",
    SelectEvents = cms.untracked.PSet(  SelectEvents = cms.vstring( 'p2' ) ),
    outputCommands = cms.untracked.vstring( 'drop *',
      'keep FEDRawDataCollection_rawDataCollector_*_*',
      'keep FEDRawDataCollection_source_*_*' )
)

process.p3 = cms.Path( process.ExceptionGenerator3 + process.HLTPrescaler3 )
process.ep3 = cms.EndPath( process.hltOutputB )
process.pDQMhisto = cms.Path( process.DQMFileSaver )
process.json = cms.EndPath( process.TriggerJSONMonitoring )
process.L1Gt = cms.Path( process.hltL1GtObjectMap )
process.ep2 = cms.EndPath( process.hltOutputDQM )
process.ep1 = cms.EndPath( process.hltOutputA )
process.p2 = cms.Path( process.ExceptionGenerator2 + process.HLTPrescaler )
process.p1 = cms.Path( process.ExceptionGenerator + process.HLTPrescaler2 )

process.transferSystem = cms.PSet(
  destinations = cms.vstring("Tier0","DQM","ECAL","None"),
  transferModes = cms.vstring("tier0_on","tier0_off","test"),
  streamA = cms.PSet(tier0_on=cms.vstring( "Tier0" ),tier0_off=cms.vstring( "None" ),test=cms.vstring( "None" )),
  streamB = cms.PSet(tier0_on=cms.vstring( "None" ),tier0_off=cms.vstring( "None" ),test=cms.vstring( "None" )),
  streamDQM = cms.PSet(tier0_on=cms.vstring( "DQM","Tier0" ),tier0_off=cms.vstring( "DQM" ),test=cms.vstring( "None" )),
  streamL1Rates = cms.PSet(tier0_on=cms.vstring( "Tier0" ),tier0_off=cms.vstring( "None" ),test=cms.vstring( "None" )),
  streamHLTRates = cms.PSet(tier0_on=cms.vstring( "Tier0" ),tier0_off=cms.vstring( "None" ),test=cms.vstring( "None" )),
  streamDQMHistograms = cms.PSet(tier0_on=cms.vstring( "DQM" ),tier0_off=cms.vstring( "DQM" ),test=cms.vstring( "None" ))
)

import FWCore.ParameterSet.VarParsing as VarParsing 

import os 

cmsswbase = os.path.expandvars('$CMSSW_BASE/') 

options = VarParsing.VarParsing ('analysis') 

options.register ('runNumber', 
                  1, # default value 
                  VarParsing.VarParsing.multiplicity.singleton, 
                  VarParsing.VarParsing.varType.int,          # string, int, or float 
                  "Run Number") 

options.register ('buBaseDir', 
                  '/fff/BU0', # default value 
                  VarParsing.VarParsing.multiplicity.singleton, 
                  VarParsing.VarParsing.varType.string,          # string, int, or float 
                  "BU base directory") 

options.register ('dataDir', 
                  '/fff/data', # default value 
                  VarParsing.VarParsing.multiplicity.singleton, 
                  VarParsing.VarParsing.varType.string,          # string, int, or float 
                  "FU data directory") 

options.register ('numThreads', 
                  1, # default value 
                  VarParsing.VarParsing.multiplicity.singleton, 
                  VarParsing.VarParsing.varType.int,          # string, int, or float 
                  "Number of CMSSW threads") 

options.register ('numFwkStreams', 
                  1, # default value 
                  VarParsing.VarParsing.multiplicity.singleton, 
                  VarParsing.VarParsing.varType.int,          # string, int, or float 
                  "Number of CMSSW streams") 

options.parseArguments() 

process.options = cms.untracked.PSet( 
    numberOfThreads = cms.untracked.uint32(options.numThreads), 
    numberOfStreams = cms.untracked.uint32(options.numFwkStreams), 
    multiProcesses = cms.untracked.PSet( 
    maxChildProcesses = cms.untracked.int32(0) 
    ) 
) 

process.PoolDBESSource.connect   = 'frontier://FrontierProd/CMS_COND_31X_GLOBALTAG'
process.PoolDBESSource.pfnPrefix = cms.untracked.string('frontier://FrontierProd/')


process.EvFDaqDirector.buBaseDir    = options.buBaseDir 
process.EvFDaqDirector.baseDir      = options.dataDir 
process.EvFDaqDirector.runNumber    = options.runNumber 
