# --------------------------------------------------------
__author__ = "Vladimir Fedorov"
__copyright__ = "Copyright (C) 2013 Migrate2Iaas"
#---------------------------------------------------------


import ConfigParser
import collections
import encodings

class UnicodeConfigParser(ConfigParser.RawConfigParser):
    """Specialized config parser. The main feature is it's unicode support"""
    
    def __init__(self, encoding = 'utf16', defaults=None, dict_type=ConfigParser._default_dict, allow_no_value=False):
        self.__encoding = encoding
        ConfigParser.RawConfigParser.__init__(self, defaults, dict_type, allow_no_value)

    def read(self, filenames):
        if isinstance(filenames, basestring):
            filenames = [filenames]
        read_ok = []
        for filename in filenames:
            try:
                fp = encodings.codecs.open(filename, 'r', self.__encoding)
            except IOError:
                continue
            self.read(fp)
            fp.close()
            read_ok.append(filename)
        return read_ok
    

    def write(self, fp):
        """Write an .ini-format representation of the configuration state."""
        if self._defaults:
            fp.write("[%s]\n" % DEFAULTSECT)
            for (key, value) in self._defaults.items():
                fp.write("%s = %s\n" % (key, unicode(value, self.__encoding).replace('\n', '\n\t')))
            fp.write("\n")
        for section in self._sections:
            fp.write("[%s]\n" % section)
            for (key, value) in self._sections[section].items():
                if key == "__name__":
                    continue
                if (value is not None) or (self._optcre == self.OPTCRE):
                    key = u" = ".join((key, unicode(value).replace(u'\n', u'\n\t')))
                fp.write("%s\n" % (key))
            fp.write("\n")