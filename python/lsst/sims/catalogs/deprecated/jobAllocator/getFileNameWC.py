# execfile('getFileNameWC.py')

import fnmatch, os

def do(dir0, str0):
    matches = []
    for file in os.listdir(dir0):
        if fnmatch.fnmatch(file, str0):
            matches.append(file)
    return matches
