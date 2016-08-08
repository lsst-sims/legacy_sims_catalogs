import sys
import os
def writeFile(fh, obshistid, radius, id):
    for obsid in obshistid:
        header = """### ---------------------------------------
### PBS script created by: krughoff
### ---------------------------------------
#PBS -S /bin/tcsh
#PBS -N krughoff
#PBS -M krughoff@astro.washington.edu
#PBS -m a
#PBS -V
#PBS -j oe
#PBS -o /share/pogo3/krughoff/pbsout/%i.out
#PBS -e /share/pogo3/krughoff/pbsout/%i.err
#PBS -l walltime=60:00:00
#PBS -l nodes=1:ppn=1
#PBS -l pmem=1000MB
#PBS -q default

### ---------------------------------------
### Begin Executable Sections
### ---------------------------------------
setenv SHELL /bin/tcsh
echo $HOME
echo `hostname`
source $HOME/.cshrc
source $HOME/setupStack.csh
"""%(id, id)
    fh.write(header)
    for obsid in obshistid:
        if os.path.exists("/share/pogo3/krughoff/testDir/obsid%s.tar.gz"%obsid):
            continue
        else:
            fh.write("python $CATALOGS_GENERATION_DIR/bin/runFiles.py %i %g\n"%(obsid, radius))
    fh.close()

if __name__ == "__main__":
    obsid = []
    for el in sys.argv[1:-1]:
        obsid.append(int(el))
    radius = float(sys.argv[-1])
    fh = open("test%i.pbs"%(obsid[0]), "w")
    writeFile(fh, obsid, radius, obsid[0])

