from boto.ec2.connection import *
from boto.ec2.volume import *

# boto code-style is used in this file

class ConversionImage(dict):

    ValidValues = ['format', 'size', 'importManifestUrl', 'checksum']

    def __init__(self, parent=None):
        dict.__init__(self)
        self.format = None
        self.size = None
        self.import_manifest_url = None
        self.checksum = None
        self._current_value = None

    def startElement(self, name, attrs, connection):
        return None

    def endElement(self, name, value, connection):
        if name == 'format':
            self.format = value
        elif name == 'size':
            self.size = value
        elif name == 'importManifestUrl':
            self.import_manifest_url = value
        elif name == 'checksum':
            self.checksum = value
        elif name in self.ValidValues:
            self[name] = self._current_value




class VolumeConversionTask(dict):
    
    ValidValues = ['bytesConverted', 'availabilityZone', 'description']

    def __init__(self, parent=None):
        dict.__init__(self)
        self.availability_zone = None
        self.bytes_converted = None
        self.description = None
        self.volume = None

        self._current_value = None

    def startElement(self, name, attrs, connection):
        if name == 'image':
            self[name] = ConversionImage()
            return self[name]
        elif name == 'volume':
            self.volume  = Volume()
            return self.volume 
        else:
            return None

    def endElement(self, name, value, connection):
        if name == 'bytesConverted':
            self.bytes_converted = value
        elif name == 'availabilityZone':
            self.availability_zone = value
        elif name == 'description':
            self.description = value
        elif name in self.ValidValues:
            self[name] = self._current_value

class ConversionTask(dict):

    ValidValues = ['conversionTaskId', 'expirationTime', 'importVolume', 'state',
                   'statusMessage']

    def __init__(self, parent=None):
        dict.__init__(self)
        self.connection = parent
        self.conversion_task_id = None
        self.expiration_time = None
        self.import_volume = None
        self.state = None
        self.status_message = None
       
        self._current_value = None

    def is_volume(self):
        return self.has_key('importVolume')

    def is_instance(self):
        return self.has_key('importInstance')

    def get_resulting_id(self):
        if self.is_volume():
            return self['importVolume'].volume.id

    def get_status(self):
        return self.state

    def get_message(self):
        return self.status_message

    def _update(self, updated):
        #NOTE: seems like child elements are not updated
        self.__dict__.update(updated.__dict__)

    def update(self, validate=False):
        """
        Update the data associated with this task by querying EC2.

        :type validate: bool
        :param validate: By default, if EC2 returns no data about the
                         task the update method returns quietly.  If
                         the validate param is True, however, it will
                         raise a ValueError exception if no data is
                         returned from EC2.
        """
        # CHILD ELEMENTS ARE NOT UPDATED, needs some fixes
        unfiltered_rs = self.connection.get_import_tasks([self.conversion_task_id])
        rs = [x for x in unfiltered_rs if x.conversion_task_id == self.conversion_task_id]
        if len(rs) > 0:
            self._update(rs[0])
        elif validate:
            raise ValueError('%s is not a valid Conversion Task ID' % self.conversion_task_id)
        return

    def startElement(self, name, attrs, connection):
        if name == 'image':
            self[name] = ConversionImage(self)
            return self[name]
        elif name == 'importVolume':
            self[name] = VolumeConversionTask(self)
            return self[name]
        elif name == 'importInstance':
            self[name] = InstanceConversionTask(self)
            return self[name]
        else:
            return None

    def endElement(self, name, value, connection):
        if name == 'conversionTaskId':
            self.conversion_task_id = value
        elif name == 'expirationTime':
            self.expiration_time = value
        elif name == 'state':
            self.state = value
        elif name == 'statusMessage':
            self.status_message = value
        elif name in self.ValidValues:
            self[name] = self._current_value


class EC2ImportConnection(boto.ec2.connection.EC2Connection):
    """ Class contatining all EC2 VM import API calls """

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None, regionname='',
                 is_secure=True, host=None, port=None,
                 proxy=None, proxy_port=None,
                 proxy_user=None, proxy_pass=None, debug=0,
                 https_connection_factory=None, region=None, path='/',
                 api_version=None, security_token=None,
                 validate_certs=True):
        """
        Init method to create a new connection to EC2.
        """
        if region == None:
            region = boto.ec2.get_region(regionname , aws_access_key_id=aws_access_key_id , aws_secret_access_key=aws_secret_access_key )
        EC2Connection.__init__(self, aws_access_key_id, aws_secret_access_key,
                              is_secure, host, port, proxy, proxy_port, proxy_user, proxy_pass, debug,
                              https_connection_factory, region, path, api_version, security_token, validate_certs)


    def import_volume(self, import_manifest_xml, imagesize_bytes , image_format , availability_zone , volume_size_gb , description=""):
        #TODO: make extra checks
        params = {'AvailabilityZone': availability_zone , 'Image.Format': image_format, 'Image.Bytes' : str(imagesize_bytes) , 'Image.ImportManifestUrl' : import_manifest_xml, 'Volume.Size' : str(volume_size_gb)}
        if description:
            params['Description']=description
        return self.get_object('ImportVolume', params,
                               ConversionTask, verb='POST')

    def get_import_tasks(self , import_task_ids=None):
        params = dict()
        if import_task_ids:
            self.build_list_params(params, import_task_ids, 'ConversionTaskId')
        return self.get_list('DescribeConversionTasks', params,
                               [('item' , ConversionTask)], verb='POST')