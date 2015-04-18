#!/bin/python
import os
import sys

filepath = ''
if len(sys.argv) > 1:
    filepath = sys.argv[1]
else:
    print ('Please specify file base e.g. WIN-xxxxxxx.')
    print ("usage : mergeparts.py <filebase> [outfile]")

outpath = 'out.img'
if len(sys.argv) > 2:
    outpath = sys.argv[2]

print ("Merging to " + outpath)

out = open(outpath, 'wb')
part = 0

while 1:
    try:
        partname = filepath+'.part'+ str(part)
        print ('openning ' +partname)
        f = open(partname , 'rb')
        data = f.read()
        out.write(data)
        f.close()
        part = part + 1
    except:
        print ('stop')
        break

out.close()
