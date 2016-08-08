import sys
#This assumes that the original file is sorted in time:
#select min(obshistid) as obshistid, min(fieldid) as fieldid, min(filter) as
#filter from output_opsim3_61 where fieldid in
#(2762,2886,2770,2536,2656,2544,2426) and night between 4*365. and
#5*365 group by expmjd order by min(expmjd) limit 450;

def getFieldFilt(fname):
    fh = open(fname)
    mapDict = {}
    for l in fh:
        if l.startswith("#"):
            continue
        (obsid, field, filter) = l.rstrip().split(',')
        key = "%s_%s"%(field,filter)
        if mapDict.has_key(key):
            mapDict[key].append(obsid)
        else:
            mapDict[key] = []
            mapDict[key].append(obsid)
    return mapDict

if __name__ == "__main__":
    mydict = getFieldFilt(sys.argv[1])
    for k in mydict.keys():
        (field,filter) = k.split("_")
        num = len(mydict[k])
        for i in range(num):
            if i < num/2.:
                print mydict[k][i], field, filter, "nocloud"
            else:
                print mydict[k][i], field, filter, "cloud"
