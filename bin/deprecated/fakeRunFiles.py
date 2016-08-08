import os, sys, time, random

if __name__ == "__main__":
    time.sleep(1)
    if random.random() > 0.66667:
        sys.exit(1)
    else:
        sys.exit(0)
