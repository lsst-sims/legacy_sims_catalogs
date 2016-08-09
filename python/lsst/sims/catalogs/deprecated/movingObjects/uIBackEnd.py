# execfile('uIBackEnd.py')
#execfile('r.py') # Reload all modules when this one is reloaded

import pg, numpy

def printSyntax():
    print '\nuIBackEnd.py syntax:'
    print '   python uIBackEnd.py qType [optional params]'
    print '\nwhere:'
    print '   qType is the type of query'
    print '      (bbox, bboxMJD, MJD, ObsHistID, cone, SQL)'
    print '\nOptional arguments (case-insensitive):'
    print '   +++ All RA and dec values are in degrees +++'
    print '   minRA=x:  minimum RA in bounding box'
    print '   maxRA=x:  maximum RA in bounding box'
    print '   minDec=x:  minimum declination in bounding box'
    print '   maxDec=x:  maximum declination in bounding box'
    print '   minMJD=x:  minimum MJD time in bounding box'
    print '   maxMJD=x:  maximum MJD time in bounding box'
    print '   centerRA=x:  RA of center of bounding circle'
    print '   centerDec=x:  Dec of center of bounding circle'
    print '   radius=x:  radius of bounding circle (degrees)'
    print '   obsHistIDNum=x:  unique obsHistID'
    print '   SQLString=x:  string for SQL query'
    print '\n\n'

def parseCommandLine():
    if len(sys.argv) < 3:
        print '*** Need at least 2 command-line parameters.'
        printSyntax()
        sys.exit(1)

    argDict = {}
    if sys.argv[1].lower() == 'bbox': argDict['inCmd'] = 'bbox'
    elif sys.argv[1].lower() == 'bbox': argDict['inCmd'] = 'bbox'
    elif sys.argv[1].lower() == 'bboxmjd': argDict['inCmd'] = 'bboxMJD'
    elif sys.argv[1].lower() == 'mjd': argDict['inCmd'] = 'mJD'
    elif sys.argv[1].lower() == 'cone': argDict['inCmd'] = 'cone'
    elif sys.argv[1].lower() == 'obshistid': argDict['inCmd'] = 'obsHistID'
    elif sys.argv[1].lower() == 'sql': argDict['inCmd'] = 'sQL'
    else: raise ValueError, '*** qType not recognized: ' + sys.argv[1]

    for i in range(2, len(sys.argv)):
        print 'Parsing optional argument: ' + sys.argv[i]
        t0 = sys.argv[i].split('=')[0].lower()
        if t0 == 'minra':
            t1 = float(sys.argv[i].split('=')[1])
            print '+++ minRA (deg) +++:  %3.7f' % t1
            argDict['minRA'] = t1
        elif t0 == 'maxra':
            t1 = float(sys.argv[i].split('=')[1])
            print '+++ maxRA (deg) +++:  %3.7f' % t1
            argDict['maxRA'] = t1
        elif t0 == 'mindec':
            t1 = float(sys.argv[i].split('=')[1])
            print '+++ minDec (deg) +++:  %3.7f' % t1
            argDict['minDec'] = t1
        elif t0 == 'maxdec':
            t1 = float(sys.argv[i].split('=')[1])
            print '+++ maxDec (deg) +++:  %3.7f' % t1
            argDict['maxDec'] = t1
        elif t0 == 'minmjd':
            t1 = float(sys.argv[i].split('=')[1])
            print '+++ minMJD (deg) +++:  %7.3f' % t1
            argDict['minMJD'] = t1
        elif t0 == 'maxmjd':
            t1 = float(sys.argv[i].split('=')[1])
            print '+++ maxMJD (deg) +++:  %7.3f' % t1
            argDict['maxMJD'] = t1
        elif t0 == 'centerra':
            t1 = float(sys.argv[i].split('=')[1])
            print '+++ centerRA (deg) +++:  %3.7f' % t1
            argDict['centerRA'] = t1
        elif t0 == 'centerdec':
            t1 = float(sys.argv[i].split('=')[1])
            print '+++ centerDec (deg) +++:  %3.7f' % t1
            argDict['centerDec'] = t1
        elif t0 == 'radius':
            t1 = float(sys.argv[i].split('=')[1])
            print '+++ radius (deg) +++:  %3.7f' % t1
            argDict['radius'] = t1
        elif t0 == 'obshistidnum':
            t1 = int(sys.argv[i].split('=')[1])
            print '+++ obsHistIDNum +++:  %i' % t1
            argDict['obsHistIDNum'] = t1
        elif t0 == 'sqlstring':
            t1 = sys.argv[i].split('=')
            t2 = t1[1]
            for i in range(2,len(t1)): t2 += ('=' + t1[i])
            print '+++ string +++:  %s' % t2
            argDict['sQLString'] = t2
        else: raise RuntimeError, '*** Unknown param: ' + t0
    return argDict

def verifyArgs(argDict):
    if not argDict.has_key('inCmd'):
        raise ValueError, '*** qType not supplied.'
    if argDict['inCmd'] == 'bbox': pass
    elif argDict['inCmd'] == 'bboxMJD': pass
    elif argDict['inCmd'] == 'mJD': pass
    elif argDict['inCmd'] == 'cone': pass
    elif argDict['inCmd'] == 'obsHistID': pass
    elif argDict['inCmd'] == 'sQL': pass
    else: raise ValueError, '*** Unknown qType: ' + argDict['inCmd']
    
    t0 = argDict['inCmd']
    if t0 == 'bbox':
        if not argDict.has_key('minRA'):
            raise ValueError, '*** minRA required for bbox'
        if not argDict.has_key('minDec'):
            raise ValueError, '*** minDec required for bbox'
        if not argDict.has_key('maxRA'):
            raise ValueError, '*** maxRA required for bbox'
        if not argDict.has_key('maxDec'):
            raise ValueError, '*** maxDec required for bbox'
    elif t0 == 'bboxMJD':
        if not argDict.has_key('minRA'):
            raise ValueError, '*** minRA required for bbox'
        if not argDict.has_key('minDec'):
            raise ValueError, '*** minDec required for bbox'
        if not argDict.has_key('maxRA'):
            raise ValueError, '*** maxRA required for bbox'
        if not argDict.has_key('maxDec'):
            raise ValueError, '*** maxDec required for bbox'
        if not argDict.has_key('minMJD'):
            raise ValueError, '*** minMJD required for bboxMJD'
        if not argDict.has_key('maxMJD'):
            raise ValueError, '*** maxMJD required for bboxMJD'
    elif t0 == 'mJD':
        if not argDict.has_key('minMJD'):
            raise ValueError, '*** minMJD required for mJD'
        if not argDict.has_key('maxMJD'):
            raise ValueError, '*** maxMJD required for mJD'
    elif t0 == 'cone':
        if not argDict.has_key('centerRA'):
            raise ValueError, '*** centerRA required for cone'
        if not argDict.has_key('centerDec'):
            raise ValueError, '*** centerDec required for cone'
        if not argDict.has_key('radius'):
            raise ValueError, '*** radius required for cone'
    elif t0 == 'obsHistID':
        if not argDict.has_key('obsHistIDNum'):
            raise ValueError, '*** obsHistIDNum required for obsHistID'
    elif t0 == 'sQL':
        if not argDict.has_key('sQLString'):
            raise ValueError, '*** sQLString required for sQLString'
    elif t0 == 'inCmd':
        pass
    else:
        raise ValueError, '*** Unknown qType: ' + t0
    print '>>> Argument verification completed. <<<'

def constructSQLWhere(argDict):
    # Assume all parameters are present and verification has passed
    toRad = numpy.pi / 180.
    outStr = 'WHERE '
    t0 = argDict['inCmd']
    if t0 == 'bbox':
        # OpSim takes RA, Dec in radians
        outStr += 'fieldRA >= %3.7f and ' % (argDict['minRA'] * toRad)
        outStr += 'fieldRA <= %3.7f and ' % (argDict['maxRA'] * toRad)
        outStr += 'fieldDec >= %3.7f and ' % (argDict['minDec'] * toRad)
        outStr += 'fieldDec <= %3.7f' % (argDict['maxDec'] * toRad)
    elif t0 == 'bboxMJD':
        outStr += 'fieldRA >= %3.7f and ' % (argDict['minRA'] * toRad)
        outStr += 'fieldRA <= %3.7f and ' % (argDict['maxRA'] * toRad)
        outStr += 'fieldDec >= %3.7f and ' % (argDict['minDec'] * toRad)
        outStr += 'fieldDec <= %3.7f and ' % (argDict['maxDec'] * toRad)
        outStr += 'expMJD >= %7.3f and ' % (argDict['minMJD'])
        outStr += 'expMJD <= %7.3f' % (argDict['maxMJD'])
    elif t0 == 'mJD':
        outStr += 'expMJD >= %7.3f and ' % (argDict['minMJD'])
        outStr += 'expMJD <= %7.3f' % (argDict['maxMJD'])
    elif t0 == 'cone':
        outStr += "scircle '<(%3.7f, %3.7f), %3.7f>' " %\
                  (argDict['centerRA']*toRad, argDict['centerDec']*toRad,
                   argDict['radius']*toRad)
        outStr += "~spoint(fieldRA, fieldDec)"
    elif t0 == 'obsHistID':
        outStr += 'obsHistID = %i' % int(argDict['obsHistIDNum'])
    elif t0 == 'sQL':
        outStr += argDict['sQLString']
    else:
        raise ValueError, '*** Unknown qType: ' + t0
    return outStr

# Start up OpSim postgres client:
#  /astro/apps/bin/psql -h deathray -U cosmouser spheretest
#  password: cosmouser
def do(argDict, selStr0):
    print 'Dumping argDict:'
    for k in argDict:
        print k, argDict[k]
    verifyArgs(argDict)
    whereStr = constructSQLWhere(argDict)
    print 'WHERE str is: \n   ' + whereStr

    fromStr = '(select min(obshistid) obshistid from output_opsim3_61 ' +\
              '%s group by expdate) a,' % whereStr
    matchStr = 'output_opsim3_61 b where a.obshistid = b.obshistid;'

    dBName0 = 'cosmoDB.11.19.2009'
    res = pg.connect(host='deathray.astro.washington.edu', user='cosmouser', dbname=dBName0, passwd='cosmouser')


    #b.rotSkyPos, b.rotTelPos, b.sunalt, b.sunaz, b.rawseeing, b.seeing,
    #b.filtsky, b.dist2moon, b.moonalt, b.phaseangle, b.miescatter,
    #b.moonillum, b.darkbright, b.perry_skybrightness from (select
    #min(obshistid) obshistid from output_opsim3_61 where fielddec between
    #-0.033 and 0.033 and fieldra between 0.161 and 6.122 and expmjd
    #between 49353. and 49718. group by expdate) a, output_opsim3_61 b
    #where a.obshistid = b.obshistid;

    query = 'SELECT %s FROM %s %s' % (
        selStr0, fromStr, matchStr)
    print 'Query: %s' % query
    results = res.query(query)
    dictRes = results.dictresult()
    print 'Got %i results' % len(dictRes)
    #dictRes['DBQueried'] = dBName
    #print dictRes
    return dictRes, dBName0

#whereStr = 'WHERE expMJD>=50000 and expMJD<=50001.1'
#res = pg.connect(host='localhost', user='krughoff', dbname='spheretest')
#query = 'select count(*) from output_opsim1_29 %s' % whereStr
#results = res.query(query)
#dictRes = results.dictresult()


def queryOnObsHistID(obsHistIdStr):
    argDict = {}
    argDict['inCmd'] = 'obsHistID'
    argDict['obsHistIDNum'] = obsHistIdStr
    (t0, t1) = do(argDict, '*')
    return t0[0]




# python uIBackEnd.py bbox minRA=10.2 minDec=10.4 maxRA=11 maxDec=12
# python uIBackEnd.py bboxMJD minRA=10.2 minDec=10.4 maxRA=11 maxDec=12 minMJD=50000 maxMJD=50011.1
# python uIBackEnd.py mJD minMJD=50000 maxMJD=50011.1
# python uIBackEnd.py cone centerRA=10.2 centerDec=10.4 radius=10.
# python uIBackEnd.py obsHistID obsHistIDNum=10217182
# python uIBackEnd.py sQL sQLString=blah=bleh

#if __name__ == '__main__':
#    argDict = parseCommandLine()
#    do(argDict)



#mysql> describe output_opsim1_29;
#+---------------------+------------------+------+-----+---------+-------+
#| Field               | Type             | Null | Key | Default | Extra |
#+---------------------+------------------+------+-----+---------+-------+
#| obsHistID           | int(10) unsigned | NO   | PRI | 0       |       | 
#| sessionID           | int(10) unsigned | NO   |     | NULL    |       | 
#| propID              | int(10) unsigned | NO   |     | NULL    |       | 
#| fieldID             | int(10) unsigned | NO   | MUL | NULL    |       | 
#| filter              | varchar(8)       | NO   | MUL | NULL    |       | 
#| seqnNum             | int(10) unsigned | YES  |     | NULL    |       | 
#| subseq              | varchar(8)       | NO   |     | NULL    |       | 
#| pairNum             | int(10) unsigned | YES  |     | NULL    |       | 
#| expDate             | int(10) unsigned | NO   |     | NULL    |       | 
#| expMJD              | double           | NO   | MUL | NULL    |       | 
#| expTime             | float            | NO   |     | NULL    |       | 
#| slewTime            | float            | NO   |     | NULL    |       | 
#| slewDist            | float            | NO   |     | NULL    |       | 
#| rotSkyPos           | float            | NO   |     | NULL    |       | 
#| rotTelPos           | float            | NO   |     | NULL    |       | 
#| fldVisits           | int(10) unsigned | NO   |     | NULL    |       | 
#| fldInt              | int(10) unsigned | NO   |     | NULL    |       | 
#| fldFltrInt          | int(10) unsigned | NO   |     | NULL    |       | 
#| propRank            | float            | NO   |     | NULL    |       | 
#| finRank             | float            | NO   |     | NULL    |       | 
#| maxSeeing           | float            | NO   |     | NULL    |       | 
#| rawSeeing           | float            | NO   |     | NULL    |       | 
#| seeing              | float            | NO   |     | NULL    |       | 
#| xparency            | float            | NO   |     | NULL    |       | 
#| cldSeeing           | float            | NO   |     | NULL    |       | 
#| airmass             | float            | NO   |     | NULL    |       | 
#| VskyBright          | float            | NO   |     | NULL    |       | 
#| filtSky             | float            | NO   |     | NULL    |       | 
#| fieldRA             | float            | NO   | MUL | NULL    |       | 
#| fieldDec            | float            | NO   | MUL | NULL    |       | 
#| lst                 | float            | NO   |     | NULL    |       | 
#| altitude            | float            | NO   |     | NULL    |       | 
#| azimuth             | float            | NO   |     | NULL    |       | 
#| dist2Moon           | float            | NO   |     | NULL    |       | 
#| moonRA              | float            | NO   |     | NULL    |       | 
#| moonDec             | float            | NO   |     | NULL    |       | 
#| moonAlt             | float            | NO   |     | NULL    |       | 
#| moonPhase           | float            | NO   |     | NULL    |       | 
#| sunAlt              | float            | NO   |     | NULL    |       | 
#| sunAz               | float            | NO   |     | NULL    |       | 
#| phaseAngle          | float            | NO   |     | NULL    |       | 
#| rScatter            | double           | NO   |     | NULL    |       | 
#| mieScatter          | float            | NO   |     | NULL    |       | 
#| moonIllum           | float            | NO   |     | NULL    |       | 
#| moonBright          | float            | NO   |     | NULL    |       | 
#| darkBright          | float            | NO   |     | NULL    |       | 
#| 5sigma              | float            | YES  |     | NULL    |       | 
#| perry_skybrightness | float            | YES  |     | NULL    |       | 
#| 5sigma_ps           | float            | YES  |     | NULL    |       | 
#| uwskybright         | float            | YES  |     | NULL    |       | 
#| x                   | float            | YES  |     | NULL    |       | 
#| y                   | float            | YES  |     | NULL    |       | 
#| z                   | float            | YES  |     | NULL    |       | 
#| 5sigma_uw           | float            | YES  |     | NULL    |       | 
#| hexdithra           | float            | YES  | MUL | NULL    |       | 
#| hexdithdec          | float            | YES  |     | NULL    |       | 
#| vertex              | int(11)          | YES  |     | NULL    |       | 
#| night               | int(11)          | YES  | MUL | NULL    |       | 
#+---------------------+------------------+------+-----+---------+-------+
