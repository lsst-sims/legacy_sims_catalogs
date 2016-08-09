#!/usr/bin/env python
import math,sys,tarfile,os,shutil
from copy import deepcopy
import exceptions,warnings
import lsst.sims.catalogs.measures.utils as mUtils
from lsst.sims.catalogs.generation.db import queryDB

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
       
def runDia(csize, obsidList, radius=2.1, outdir='.', repodir=None, je=None, compress=True, cleanup=False):
    if repodir is None:
        repodir = outdir
    meta = None
    opsimid = None
    writeJobEvent(je, 'start')
    cattype = "DIASOURCE"
    objtype = 'SSM'
    warnings.simplefilter('ignore', category=exceptions.UserWarning)
    arcroot = "diaBatchStarting_%i"%(obsidList[0])
    outBase = os.path.join(outdir, arcroot)
    if cleanup:
        cleanUpDirs(outBase, je)
    if not os.path.exists(outBase):
        os.makedirs(outBase)
    writeJobEvent(je, 'MakeDirs', 'Made output directories %s'%(outBase))
    nid = 0
    for obsid in obsidList:
        writeJobEvent(je, 'Obshistid:%s'%(obsid), 'Doing %i out of %i total'%(nid, len(obsidList)))
        filename = "dia_%i.dat"%(obsid)
        outfile = os.path.join(outBase,filename)
        myqdb = queryDB.queryDB(chunksize=csize,objtype=objtype,filetypes=(cattype,))
        ic = myqdb.getInstanceCatalogById(obsid, radiusdeg=radius)        
        if opsimid is None:
            opsimid = myqdb.opsim
        cnum = 0
        while ic is not None:
            writeJobEvent(je, 'GetChunk', 'Got chunk #%i of length %i'%(cnum, len(ic.dataArray[ic.dataArray.keys()[0]])))
            numRec = len(ic.dataArray[ic.dataArray.keys()[0]])
            if cnum == 0:
                ic.metadata.validateMetadata(cattype, opsimid)
                ic.metadata.writeMetadata(outfile, cattype, opsimid, newfile=True, filelist=None, compress=compress)
                writeJobEvent(je, 'WriteMetadata', 'Wrote metadata to %s'%(outfile))
            ic.validateData(cattype)
            ic.writeCatalogData(outfile, cattype, newfile = False, compress=compress)
            writeJobEvent(je, 'WriteChunk', 'Wrote chunk #%i of length %i'%(cnum,numRec))
            ic = myqdb.getNextChunk()
            cnum += 1
        myqdb.closeSession()
        nid += 1
    mvFiles(repodir, outBase, arcroot, je=je)
    if cleanup:
        cleanUpDirs(outBase, je)
    writeJobEvent(je, 'stop')
