#!/usr/bin/python


import sys, getopt

outputfile = ''
number = 0


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
    fileHandle.write(factorial+"\n")
    fileHandle.flush()
    fileHandle.close()



def main(argv):
    global number, outputfile
    try:
        opts, args = getopt.getopt(argv,"hn:o:",["num=","ofile="])
    except getopt.GetoptError:
        print 'factorial.py -n <number> -o <outputfile>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'factorial.py -n <number> -o <outputfile>'
            sys.exit()
        elif opt in ("-n", "--num"):
            number = int(arg)
        elif opt in ("-o", "--ofile"):
            outputfile = arg
    print number, outputfile


if __name__ == "__main__":
   main(sys.argv[1:])
   print number, outputfile, str(factorial(number))
   writeFile(str(factorial(number)))


