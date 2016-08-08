#!/usr/bin/env python
import math,sys,tarfile,os,shutil
from copy import deepcopy
import exceptions,warnings
import lsst.sims.catalogs.measures.utils as mUtils
from lsst.sims.catalogs.generation.db import queryDB

# same for all catalog jobs
def writeJobEvent(je, event, description=''):
    if je is not None:
        if event == 'start':
            je.registerTaskStart()
        elif event == 'stop':
            je.registerTaskStop()
        else:
            je.registerEvent(event, eventdescription=description)

def mvFiles(repodir, basedir, arcroot, je = None):
    if not os.path.exists(repodir):
        os.makedirs(repodir)
    writeJobEvent(je, "MoveFiles", "Moving files for to %s"%(arcroot))
    tar = tarfile.open(os.path.join(repodir,"%s.tar.gz"%(arcroot)), "w:gz")
    tar.add(basedir, arcname=arcroot)
    writeJobEvent(je, "MoveFiles", "Added file %s"%(basedir))
    tar.close()

def cleanUpDirs(dirs, je = None):
    if os.path.exists(dirs):
        shutil.rmtree(dirs)
        writeJobEvent(je, "RemoveDirs", "Cleaned up by deleting %s"%(dirs))
    else:
        writeJobEvent(je, "RemoveDirs", "Directory %s does not exist"%(dirs))

# specific to calibration catalog generation (apart from bookkeeping; that's generic)   
def runCalib(csize, obsid, radius=2.1, outdir='.', repodir=None, je=None, compress=True, cleanup=False):
    if repodir is None:
        repodir = outdir
    meta = None
    opsimid = None
    files = []
    writeJobEvent(je, 'start')
    cattype = "CALIB"
    print "Warning -- only MSSTARS in calib cats currently"
    #objtypes = ['MSSTARS','WDSTARS','BHBSTARS','RRLYSTARS', \
    #            'GLENS','IMAGE','EBSTARS','CEPHEIDSTARS','EASTEREGGS','GALAXY_BULGE','GALAXY_DISK','AGN']
    #varobj = ['MSSTARS', 'RRLYSTARS', 'AGN', 'IMAGE', 'WDSTARS', 'EBSTARS', 'CEPHEIDSTARS']    
    objtypes = ['MSSTARS', ]
    varobj = ['MSSTARS',]
    warnings.simplefilter('ignore', category=exceptions.UserWarning)
    arcroot = "obsid%i"%(obsid)
    outBase = os.path.join(outdir, arcroot)
    if cleanup:
        cleanUpDirs(outBase, je)
    subdir = "pops"
    popsPath = os.path.join(outBase, subdir)
    if not os.path.exists(popsPath):
        os.makedirs(popsPath)
    writeJobEvent(je, 'MakeDirs', 'Made output directories %s'%(popsPath))
    files = []
    for objtype in objtypes:
        writeJobEvent(je, 'Object:%s'%(objtype), 'Doing %s out of: %s'%(objtype, ",".join(objtypes)))
        filename = "trim_%i_%s.dat"%(obsid,objtype)
        outfile = os.path.join(popsPath,filename)
        myqdb = queryDB.queryDB(chunksize=csize,objtype=objtype)
        # This grabs the metadata and grabs the catalog in a circle around central pointing.
        #  WHY COMBINED? DO WE NEED TO SPLIT TO PROVIDE ADDITIONAL METADATA EASILY?
        ic = myqdb.getInstanceCatalogById(obsid, radiusdeg=radius)        
        if opsimid is None:
            opsimid = myqdb.opsim
        cnum = 0
        while ic is not None:
            writeJobEvent(je, 'GetChunk', 'Got chunk #%i of length %i'%(cnum, len(ic.dataArray[ic.dataArray.keys()[0]])))
            # Calculate focal plane mm/mm and chip x/y for each object.
            ic.makeXyCoords()
            writeJobEvent(je, 'MakeXY', 'Made xy coords for chunk #%i'%(cnum))
            # Calculate calib counts for each object.
            ic.calcCalibCounts()
            writeJobEvent(je, 'calcCalibCounts', 'Generated throughput curve and calculated counts for chunk #%i' %(cnum))
            if cnum == 0:
                if compress:
                    files.append(os.path.join(subdir,filename)+".gz")
                else:
                    files.append(os.path.join(subdir,filename))
                # Okay, need to understand this a little more ... think it's mostly formatting/writing metadata.
                # NEED TO WRITE A NEW VERSION?
                mUtils.trimGeneration.derivedTrimMetadata(ic)
                if meta is None:
                    meta = deepcopy(ic.metadata)
                else:
                    meta.mergeMetadata(ic.metadata)
            if objtype in varobj:
                writeJobEvent(je, 'DoVar', 'Applying variability to chunk #%i of type %s'%(cnum, objtype))
                ic.applyVariability()
            # WILL NEED SOMETHING HERE FOR CALIB CAT
            ic.validateData('TRIM')
            numRec = len(ic.dataArray[ic.dataArray.keys()[0]])
            if cnum == 0:
                ic.writeCatalogData(outfile, "TRIM", newfile = True, compress=compress)
                writeJobEvent(je, 'WriteChunk', 'Wrote first chunk of length %i'%(numRec))
            else:
                ic.writeCatalogData(outfile, "TRIM", newfile = False, compress=compress)
                writeJobEvent(je, 'WriteChunk', 'Wrote chunk #%i of length %i'%(cnum,numRec))
            if numRec == csize:
                ic = myqdb.getNextChunk()
            else:
                ic = None
            cnum += 1
        writeJobEvent(je, 'Finished Object:%s'%(objtype), 'Finished object %s'%(objtype))
    meta.validateMetadata(cattype, opsimid)
    metaOutfile = os.path.join(outBase,"metadata_%i.dat"%(obsid))
    meta.writeMetadata(metaOutfile, cattype, opsimid, newfile=True, filelist=files, compress=False)
    #files.append(os.path.join("obsid%i"%(obsid),"metadata_%i.dat"%(obsid)))
    writeJobEvent(je, 'WriteMetadata', 'Wrote metadata to %s'%(metaOutfile))
    mvFiles(repodir, outBase, arcroot, je=je)
    if cleanup:
        cleanUpDirs(outBase, je)
    writeJobEvent(je, 'stop')
