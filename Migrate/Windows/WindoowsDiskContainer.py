import sys
sys.path.append(sys.path[0]+'\\..')

import TransferBackingContainer

class WinDiskContainer(TransferBackingContainer.TransferBackingContainer):
    """The container is Windows disk device nevertheless of its backing store (iScsi,VHD, etc)"""


    def __init__(self , drivepath):
        self.__DrivePath = drivepath
        self.__freeSize
        self.__wholeSize

    # the disk is formated and new volume is generated
    def createTransferTarget(size):

        return
        #TODO: place create and format the disk of appropriate size

    # to write directly the partitioning schemes
    def writeRawMetaData(metadataExtents):
        return

    # to read the partitioning schemes
    def readRawMetaData(metadataExtent):
        return

