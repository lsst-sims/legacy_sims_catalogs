unsetenv PYTHONPATH
source /share/apps/lsst_gcc440/loadLSST.csh
##eups declare -r /share/pogo1/dC3b/pT2/throughputsSVN throughputs 1.0
setenv CAT_SHARE_DATA /share/pogo3/krughoff/shared/
setup -r ./generation/branches/mssql/
setup -r ./measures/trunk/
setup throughputs
setup python
setup numpy
setup pyfits

