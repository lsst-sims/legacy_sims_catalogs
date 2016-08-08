import sys
def writeFile(fh, list, radius):
    header = """### ---------------------------------------
### PBS script created by: krughoff
### ---------------------------------------
#PBS -S /bin/tcsh
#PBS -N krughoff
#PBS -M krughoff@astro.washington.edu
#PBS -m a
#PBS -V
#PBS -j oe
#PBS -o /share/pogo3/krughoff/pbsout/%s.out
#PBS -e /share/pogo3/krughoff/pbsout/%s.err
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
python $CATALOGS_GENERATION_DIR/bin/runDiaCats.py $CATALOGS_GENERATION_DIR/bin/%s %g
"""%(list, list, list, radius)
    fh.write(header)
    fh.close()

if __name__ == "__main__":
    listname = sys.argv[1]
    radius = float(sys.argv[2])
    fh = open("run%s.pbs"%(listname), "w")
    writeFile(fh, listname, radius)

