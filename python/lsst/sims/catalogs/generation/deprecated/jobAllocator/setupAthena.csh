source /share/apps/lsst_gcc440/loadLSST.csh
#setenv CAT_SHARE_DATA /share/pogo1/dC3b/pT2/simsSharedAthena/
setenv CAT_SHARE_DATA /share/pogo3/krughoff/shared/
#setenv SED_DATA /share/pogo1/dC3b/pT2/simsSharedAthena/data
#setenv CAT_SHARE_DATA /share/sdata1/krughoff/catalogs_share
#setup -r /share/home/rgibson/sims/catalogs/generation/trunk/
setup -r /share/home/rgibson/sims/catalogs/measures/trunk/
setup -r /share/home/rgibson/sims/catalogs/generation/branches/mssql/
setup -r /share/pogo3/rgibson/aASAGN/simCode/throughputs/
setup subversion
setup pyfits
unsetup numpy
setup python

