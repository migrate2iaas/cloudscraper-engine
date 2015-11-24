"""
This file defines upload to OpenStack 
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2015 Migrate2Iaas"
#---------------------------------------------------------


import sys

sys.path.append('.\..')
sys.path.append('.\..\OpenStack')

sys.path.append('.\OpenStack')


import os 

from OpenStack import GlanceUploadChannel
from OpenStack import SwiftUploadChannel
from OpenStack import SwiftUploadChannel_new

import ChainUploadChannel


class OpenStackUploadChannel(ChainUploadChannel.ChainUploadChannel):
    """
    Upload channel for Swift to Glance upload implementation
    """

    def __init__(self, result_disk_size_bytes, server_url , tennant_name , username , password, disk_format = "raw", \
        image_name="cloudscraper-image", resume_upload = False , chunksize=10*1024*1024 , container_format="bare", \
                 swift_server_url = None , swift_tennant_name = None , swift_username = None , swift_password = None , disk_name = "", container_name = "cloudscraper-container" , compression = False, \
                 upload_threads=10, use_new_channel=False ):
        """constructor"""
        super(OpenStackUploadChannel, self).__init__()

        if not swift_server_url:
           swift_server_url = server_url
        if not swift_tennant_name:
           swift_tennant_name = tennant_name 
        if not swift_username:
           swift_username = username
        if not swift_password:
           swift_password = password 
        if not disk_name:
            disk_name = image_name + "." + disk_format

        if use_new_channel:
            swift = SwiftUploadChannel_new.SwiftUploadChannel_new(result_disk_size_bytes, swift_server_url , swift_username , swift_tennant_name , swift_password , disk_name , container_name , compression ,\
                resume_upload = resume_upload , chunksize=chunksize , upload_threads=upload_threads  )
        else:
            swift = SwiftUploadChannel.SwiftUploadChannel(result_disk_size_bytes, swift_server_url , swift_username , swift_tennant_name , swift_password , disk_name , container_name , compression ,\
                resume_upload = resume_upload , chunksize=chunksize , upload_threads=upload_threads  )
        glance = GlanceUploadChannel.GlanceUploadChannel(result_disk_size_bytes, server_url , tennant_name , username , password,\
            disk_format = disk_format, image_name=image_name, container_format = container_format)
        self.appendChannel(swift)
        self.appendChannel(glance)

 