import argparse

def getIds(offset, number, fhids):
  ids = []
  for i,line in enumerate(fhids):
    if i >= offset and i < offset+number:
      flds = line.rstrip().split()
      ids.append(int(flds[0]))
  fhids.close()
  return ids

def run(id, outdir, repodir, csize=50000, radius=2.1, compress=True, cleanup=False):
    import lsst.sims.catalogs.generation.utils.runTrimCat as rtc
    try:
        rtc.runTrim(
          csize, id, radius, outdir, repodir, None, compress=compress,
          cleanup=cleanup, objtypes=['MYSTARS'])
    except Exception, e:
        print "An error occured: %s"%(e)

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="Make a trim file")
  parser.add_argument('obsid', action="store", type=int,
                   help='Obshistid for OpSim reference simulation 3.61')
  parser.add_argument('radiusdeg', action="store", type=float,
                   help='Radius of the catalog in degrees')
  parser.add_argument('outdir', action="store", type=str,
                   help='Location of working directory for trim generation')
  parser.add_argument('repodir', action="store", type=str,
                   help='Location of the final storage location of the trim file')
  args = parser.parse_args()
  run(args.obsid, outdir=args.outdir, repodir=args.repodir, radius=args.radiusdeg, csize=20000, cleanup=True)
