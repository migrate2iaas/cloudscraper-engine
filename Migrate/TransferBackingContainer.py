
class TransferBackingContainer(object):
    """Abstraction for a backing container where all the transfers are going to"""
    
    def createTransferTarget(size):
         raise NotImplementedError

#NOTE: make better interface so the transfer target may communicate with container
# and it's auto-uploading features

