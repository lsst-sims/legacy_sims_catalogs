#!/bin/csh
 
# Name of my job:
#PBS -N runJobAllocator
 
# Run for 24 hours:
#PBS -l walltime=23:59:59
 
# Where to write stderr:
#PBS -e myprog.err
 
# Where to write stdout: 
#PBS -o myprog.out
 
# Send me email when my job aborts, begins, or ends
#PBS -m abe
 
# This command switched to the directory from which the "qsub" command was run:
cd $PBS_O_WORKDIR
 
#  Now run my program
python jobAllocatorRun.py $1 $2 $3 $4

