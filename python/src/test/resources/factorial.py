#!/usr/bin/python


import sys, getopt, os

os.getcwd()
outputfile = 'target/factorial-result.txt'
number = 5


def factorial(num):
    if num <= 0:
        return 1
    else:
        factorial = 1
        for i in range(1,num + 1):
            factorial = factorial*i
        return factorial

def writeFile(factorial):
    fileHandle  = open(outputfile,'w')
    fileHandle.write(factorial+'\n')
    fileHandle.flush()
    fileHandle.close()


if __name__ == "__main__":
   writeFile(str(factorial(number)))


