"""
virtualmachine
~~~~~~~~~~~~~~~~~

This module provides virtualmachine class. 
Written in Azure Python SDK style
"""

# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------

import logging
import traceback

from win32com.client import Dispatch


# If you just double-click a pfx file, it probably ended up in CURRENT_USER\MY.
# If you have only one client certificate installed, you can omit the call to
# SetClientCertificate, and it will probably find the right one.
# Note that the last component is the Subject, not the Friendly Name.

class azure_mgmt_response(object):
    """response compatible with requests Response class"""

    def __init__(self , status , status_text , response_headers , response_body):
        self.status_code = status
        self.status = status_text
        self.headers = response_headers
        self.reason = response_body

    def __repr__(self):
        return '<Response [%s]>' % (self.statusText)

    def __bool__(self):
        """Returns true if :attr:`status_code` is 'OK'."""
        return self.ok

    def __nonzero__(self):
        """Returns true if :attr:`status_code` is 'OK'."""
        return self.ok

    def __iter__(self):
        """Allows you to use a response as an iterator."""
        return self.iter_content(128)

    @property
    def ok(self):
        try:
            self.raise_for_status()
        except Exception:
            return False
        return True

    def raise_for_status(self):
        """Raises stored :class:`HTTPError`, if one occurred."""

        http_error_msg = ''

        if 400 <= self.status_code < 500:
            http_error_msg = '%s Client Error: %s' % (self.status_code, self.reason)

        elif 500 <= self.status_code < 600:
            http_error_msg = '%s Server Error: %s' % (self.status_code, self.reason)

        if http_error_msg:
            raise RuntimeError(http_error_msg)



def send_cert_request(url, verb, cert_selection, parms = dict() , x_ms_version = "2012-03-01"):
    req = Dispatch("WinHttp.WinHttpRequest.5.1")
    req.Open(verb, url)
    req.SetClientCertificate(cert_selection) # Store and user are taken by default if not specified in the same string
    # seems like it'll be an buttheart in case we work in context of other user
    req.SetRequestHeader('x-ms-version', x_ms_version)
    for (key, value) in parms.items:
        req.SetRequestHeader(key, value)
    req.Send()
    response = azure_mgmt_response( req.Status, req.StatusText, req.GetAllResponseHeaders() ,  unicode(req.ResponseBody) )
    return response


class virtualmachine(object):
    """virtual machine service connection"""

    def __init__ (self , cert_selection, subscription_id, azure_url = "https://management.core.windows.net"):
        """
        
        Args:
            cert_selection: str - string to select management certificate in form <Location>\<store>\<cert-subject> or <cert-subj> for default location and store
            subscription_id: str - guid identifying an Azure subscription you are to manage
            azure_url: str - path to azure default url
        """

        self.__azureUrl = azure_url
        self.__subscription = subscription_id
        self.__baseUrl = azure_url+"/"+self.__subscription

        self.__certSelection = cert_selection

    def add_disk(self, disk_label , media_link , disk_name , os_type = "Windows"):
        """
        Adds disk to the user disk storage. 
        See http://msdn.microsoft.com/en-us/library/windowsazure/jj157178.aspx
        """
        operation = "services/disks"
        verb = "POST"

        url = self.__baseUrl + operation

        xmlheader = '<Disk xmlns="http://schemas.microsoft.com/windowsazure" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">\n'
        xmlbody = '<OS>'+os_type+'</OS>\n'
        xmlbody = xmlbody + '<Label>'+disk_label+'</Label>\n'
        xmlbody = xmlbody + '<MediaLink>'+media_link+'</MediaLink>\n'
        xmlbody = xmlbody + '<Name>'+disk_name+'</Name>\n'
        
        xml = xmlheader+xmlbody+'</Disk>'

        return send_cert_request(url , verb , self.__certSelection)

