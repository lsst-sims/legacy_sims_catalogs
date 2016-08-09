#!/bin/csh
#PBS -e runAJobExample.err
#PBS -o runAJobExample.out
cd $PBS_O_WORKDIR
echo Setup...
source setupAthena.csh
echo test_JA...
python runAJob.py 85748128 10 /share/sdata1/rgibson/jobAllocator/ 3
echo Finished.
