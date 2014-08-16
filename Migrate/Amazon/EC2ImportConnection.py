from boto.ec2.connection import *
from boto.ec2.volume import *
from boto.ec2.regioninfo import *
from boto.resultset import ResultSet
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


class ImportInstanceVolumeConversionTask(dict):
    ValidValues = ['bytesConverted', 'availabilityZone', 'description', 'status' , 'statusMessage']

    def __init__(self, parent=None):
        dict.__init__(self)
        self.platform = None
        self.description = None
        self.status = None
        self.status_message = None
        self.availability_zone = None
        self.bytes_converted = None
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
        elif name == 'status':
            self.status = value
        elif name == 'statusMessage':
            self.status_message = value
        elif name in self.ValidValues:
            self[name] = self._current_value


class InstanceConversionTask(dict):
    ValidValues = ['volumes', 'instanceId', 'description', 'platform']

    def __init__(self, parent=None):
        dict.__init__(self)
        self.platform = None
        self.description = None
        self.instance_id = None
        self.volume = None

        self._current_value = None

    def startElement(self, name, attrs, connection):
        if name == 'volumes':
            self.volumes  = ResultSet([('item', Volume)])
            return self.volumes 
        else:
            return None

    def endElement(self, name, value, connection):
        if name == 'instanceId':
            self.instance_id = value
        elif name == 'platform':
            self.platform = value
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

    # dumb if-polymorphism, thought nothing can be done to improve it

    def is_volume(self):
        return self.has_key('importVolume')

    def is_instance(self):
        return self.has_key('importInstance')

    def get_resulting_id(self):
        if self.is_volume():
            return self['importVolume'].volume.id
        if self.is_instance():
            return self['importInstance'].instance_id

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


class EucaConversionTask(dict):

    ValidValues = ['euca:conversionTaskId', 'euca:expirationTime', 'euca:importVolume', 'euca:state',
                   'euca:statusMessage']

    def __init__(self, parent=None):
        dict.__init__(self)
        self.connection = parent
        self.conversion_task_id = None
        self.expiration_time = None
        self.import_volume = None
        self.state = None
        self.status_message = None
       
        self._current_value = None

    # dumb if-polymorphism, thought nothing can be done to improve it

    def is_volume(self):
        return self.has_key('euca:importVolume')

    def is_instance(self):
        return self.has_key('euca:importInstance')

    def get_resulting_id(self):
        if self.is_volume():
            return self['euca:importVolume'].volume.id
        if self.is_instance():
            return self['euca:importInstance'].instance_id

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
        if name == 'euca:image':
            self[name] = ConversionImage(self)
            return self[name]
        elif name == 'euca:importVolume':
            self[name] = VolumeConversionTask(self)
            return self[name]
        elif name == 'euca:importInstance':
            self[name] = InstanceConversionTask(self)
            return self[name]
        else:
            return None

    def endElement(self, name, value, connection):
        if name == 'euca:conversionTaskId':
            self.conversion_task_id = value
        elif name == 'euca:expirationTime':
            self.expiration_time = value
        elif name == 'euca:state':
            self.state = value
        elif name == 'euca:statusMessage':
            self.status_message = value
        elif name in self.ValidValues:
            self[name] = self._current_value



class EucaRegion(boto.ec2.regioninfo.EC2RegionInfo):
    def __init__(self , host):
        EC2RegionInfo.__init__(self , None , host , host)

class EC2ImportConnection(boto.ec2.connection.EC2Connection):
    """ Class contatining all EC2 VM import API calls """

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None, regionname='',
                 is_secure=True, host=None, port=None,
                 proxy=None, proxy_port=None,
                 proxy_user=None, proxy_pass=None, debug=0,
                 https_connection_factory=None, region=None, path='/',
                 api_version=None, security_token=None,
                 validate_certs=True, eucalyptus=False):
        """
        Init method to create a new connection to EC2.
        """
        self.__eucalyptus = False
        if region == None and eucalyptus == False:
            region = boto.ec2.get_region(regionname , aws_access_key_id=aws_access_key_id , aws_secret_access_key=aws_secret_access_key )
        if eucalyptus:
            region = EucaRegion(host)
            self.__eucalyptus = True
        #    api_version = '2014-02-14'
        EC2Connection.__init__(self, aws_access_key_id, aws_secret_access_key,
                              is_secure, host, port, proxy, proxy_port, proxy_user, proxy_pass, debug,
                              https_connection_factory, region, path, api_version, security_token, validate_certs)


    def import_volume(self, import_manifest_xml, imagesize_bytes , image_format , availability_zone , volume_size_gb , description=""):
        #TODO: make extra checks
        params = {'AvailabilityZone': availability_zone , 'Image.Format': image_format, 'Image.Bytes' : str(imagesize_bytes) , 'Image.ImportManifestUrl' : import_manifest_xml, 'Volume.Size' : str(volume_size_gb)}
        if description:
            params['Description']=description
        task_class = ConversionTask
        if self.__eucalyptus:
            task_class = EucaConversionTask
        return self.get_object('ImportVolume', params,
                               task_class, verb='POST')

    def get_import_tasks(self , import_task_ids=None):
        params = dict()
        if import_task_ids:
            self.build_list_params(params, import_task_ids, 'ConversionTaskId')
        task_class = ConversionTask
        itemname = 'item'
        if self.__eucalyptus:
            task_class = EucaConversionTask
            itemname = 'euca:item'
        return self.get_list('DescribeConversionTasks', params,
                               [(itemname , task_class)], verb='POST')

    def import_instance(self, import_manifest_xml, imagesize_bytes , image_format , availability_zone , volume_size_gb , security_group , instance_type , architecture='x86_64' , description="" , vpc_subnet="" , os_platform='Windows'):
        #TODO: make extra checks
        params = {'LaunchSpecification.Placement.AvailabilityZone': availability_zone ,  'LaunchSpecification.Architecture':  architecture , 'LaunchSpecification.InstanceType' : instance_type , 'LaunchSpecification.GroupName.1' : security_group , 'DiskImage.1.Image.Format': image_format, 'DiskImage.1.Image.Bytes' : str(imagesize_bytes) , 'DiskImage.1.Image.ImportManifestUrl' : import_manifest_xml, 'DiskImage.1.Volume.Size' : str(volume_size_gb) , 'Platform' : os_platform}
        if vpc_subnet:
            params['LaunchSpecification.SubnetId'] = vpc_subnet
        if description:
            params['Description']=description
        task_class = ConversionTask
        if self.__eucalyptus:
            task_class = EucaConversionTask
        return self.get_object('ImportInstance', params,
                               task_class, verb='POST')


