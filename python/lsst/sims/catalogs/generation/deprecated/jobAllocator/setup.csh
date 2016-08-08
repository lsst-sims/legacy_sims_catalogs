setenv LSST_DEVEL /astro/net/pogo1/dC3b/pT2/lsstsandbox
unsetenv PYTHONPATH
source /astro/apps/lsst_gcc44/loadLSST.csh
setenv CAT_SHARE_DATA /astro/net/lsst1/shared/

setup -r /astro/users/rgibson/work/sims/catalogs/measures/trunk/
setup -r /astro/users/rgibson/work/sims/catalogs/generation/branches/mssql/
setup pyfits
setup matplotlib
unsetup numpy
setup python
