from azure.common import AzureMissingResourceHttpError, AzureException
from azure.storage.table import TableService, Entity
from azure.storage.queue import QueueService
from server.helper import safe_cast

import datetime
""" configure logging """
from config import log
log.name = '{}.{}'.format(log.name,__name__)

class StorageContext():
    """Initializes the repository with the specified settings dict.
        Required settings in config dict are:
        - AZURE_STORAGE_NAME
        - STORAGE_KEY
    """
    
    tables = []
    queues = []
    tableservice = TableService
    queueservice = QueueService
    storage_key = ''
    storage_name = ''

    def __init__(self, **kwargs):

        self.storage_name = kwargs.get('AZURE_STORAGE_NAME', '')
        self.storage_key = kwargs.get('AZURE_STORAGE_KEY', '')

        """ service init """
        self.tables = []
        if self.storage_key != '' and self.storage_name != '':
            self.tableservice = TableService(self.storage_name, self.storage_key)
            self.queueservice = QueueService(self.storage_name, self.storage_key)
        else:
            self.tableservice = TableService
            self.queueservice = QueueService
        pass
     
    def create_table(self, tablename) -> bool:
        if (not tablename in self.tables) and (not self.tableservice is None):
            try:
                self.tableservice.create_table(tablename)
                self.tables.append(tablename)
                return True
            except AzureException as e:
                log.error('failed to create {} with error {}'.format(tablename, e))
                return False
  
        else:
            return True
        pass

    def table_isempty(self, tablename, PartitionKey='', RowKey = '') -> bool:
        if (tablename in self.tables) and (not self.tableservice is None):

            filter = "PartitionKey eq '{}'".format(PartitionKey) if PartitionKey != '' else ''
            if filter == '':
                filter = "RowKey eq '{}'".format(RowKey) if RowKey != '' else ''
            else:
                filter = filter + ("and RowKey eq '{}'".format(RowKey) if RowKey != '' else '')
            try:
                entities = list(self.tableservice.query_entities(tablename, filter = filter, select='PartitionKey', num_results=1))
                if len(entities) == 1: 
                    return False
                else:
                    return True

            except AzureMissingResourceHttpError as e:
                log.debug('failed to query {} with error {}'.format(tablename, e))
                return True

        else:
            return True
        pass

    def create_queue(self, queuename) -> bool:
        if (not queuename in self.queues) and (not self.queueservice is None):
            try:
                self.queueservice.create_queue(queuename)
                self.queues.append(queuename)
                return True
            except AzureException as e:
                log.error('failed to create {} with error {}'.format(queuename, e))
                return False
  
        else:
            return True
        pass

    def register_model(self, storagemodel):
        if isinstance(storagemodel, StorageTableEntity):
            self.create_table(storagemodel._tablename)
        pass


    pass

class StorageTableModel(object):
    _tablename = ''
    _dateformat = ''
    _datetimeformat = ''

    def __init__(self, tableservice:TableService, **kwargs):                  
        """ constructor """
        
        self._tableservice = tableservice
        self._existsinstorage = None
        self._tablename = self.__class__._tablename
        self._dateformat = self.__class__._dateformat
        self._datetimeformat = self.__class__._datetimeformat

                
        """ parse **kwargs into instance var """
        self._PartitionKey = kwargs.get('PartitionKey', '')
        self._RowKey = kwargs.get('RowKey', '')

        for key, default in vars(self.__class__).items():
            if not key.startswith('_') and key != '':
                if key in kwargs:
                   
                    value = kwargs.get(key)
                    to_type = type(default)
                
                    if to_type is StorageTableCollection:
                        setattr(self, key, value)

                    elif to_type is datetime.datetime:
                        setattr(self, key, safe_cast(value, to_type, default, self._datetimeformat))

                    elif to_type is datetime.date:
                        setattr(self, key, safe_cast(value, to_type, default, self._dateformat))

                    else:
                        setattr(self, key, safe_cast(value, to_type, default))
                
                else:
                    setattr(self, key, default)

        """ set primary keys from data"""
        if self._PartitionKey == '':
            self.__setPartitionKey__()

        if self._RowKey == '':
            self.__setRowKey__()

        """ init Storage Table Collections in Instance """
        self.__setcollections__()
        pass
           
    def __setPartitionKey__(self):
        """ parse storage primaries from instance attribute 
            overwrite if inherit this class
        """
        pass

    def __setRowKey__(self):
        """ parse storage primaries from instance attribute 
            overwrite if inherit this class
        """
        pass

    def __setcollections__(self):
        """ initialize StorageTable collections  
            overwrite if inherit this class
        """
        pass

    def __image__(self, entity=False) -> dict:        
        """ parse self into entity """
        
        image = {}
        if entity:
            image['PartitionKey'] = self._PartitionKey
            image['RowKey'] = self._RowKey

        for key, value in vars(self).items():
            if not key.startswith('_') and key != '':
                default = getattr(self.__class__, key, None)
                istype = type(default)
                if not default is None:
                    if entity:
                        if (value != default) and (istype not in [list, dict, StorageTableCollection]):
                            image[key] = value                   
                    
                    elif istype == StorageTableCollection:
                        image[key] = getattr(self, key).dictionary()

                    else:
                        image[key] = value                    
        
        return image
            
    def dictionary(self) -> dict:
        """ representation as dictionary """
        return self.__image__()

    def exists(self) -> bool:
        
        if isinstance(self._existsinstorage, bool):
            return self._existsinstorage
        
        else:
            try:
                entity = self._tableservice.get_entity(self._tablename,self._PartitionKey, self._RowKey)
                self._existsinstorage = True
                return self._existsinstorage
            
            except AzureMissingResourceHttpError:
                self._existsinstorage = False
                return self._existsinstorage

    def load(self):
        """ load entity data from storage and merge vars in self """
        try:
            entity = self._tableservice.get_entity(self._tablename, self._PartitionKey, self._RowKey)
            self._existsinstorage = True
        
            """ sync with entity values """
            for key, default in vars(self.__class__).items():
                if not key.startswith('_') and key != '':
                    if isinstance(default, StorageTableCollection):
                        collection = getattr(self, key)
                        collection.setfilter()
                        collection.load()
                    else:
                        value = getattr(entity, key, None)
                        if not value is None:
                            oldvalue = getattr(self, key, default)
                            if oldvalue == default:
                                setattr(self, key, value)
             
        except AzureMissingResourceHttpError as e:
            log.debug('can not get table entity:  Table {}, PartitionKey {}, RowKey {} because {!s}'.format(self._tablename, self._PartitionKey, self._RowKey, e))
            self._existsinstorage = False

    def save(self, syncwithstorage=True):
        """ insert or merge self into storage """

        if syncwithstorage:
            """ try to merge entry """
            try:            
                self._tableservice.insert_or_merge_entity(self._tablename, self.__image__(entity=True))
                
                """ sync self """
                self.load()

            except AzureMissingResourceHttpError as e:
                log.error('can not insert or merge table entity:  Table {}, PartitionKey {}, RowKey {} because {!s}'.format(self._tablename, self._PartitionKey, self._RowKey, e))

        else:
            """ try to replace entry """
            try:            
                self._tableservice.insert_or_replace_entity(self._tablename, self.__image__(entity=True))
                self._existsinstorage = True

            except AzureMissingResourceHttpError as e:
                log.debug('can not insert or replace table entity:  Table {}, PartitionKey {}, RowKey {} because {!s}'.format(self._tablename, self._PartitionKey, self._RowKey, e))

    def delete(self):
        """ delete existing Entity """
        try:
            self._tableservice.delete_entity(self._tablename, self._PartitionKey, self._RowKey)
            self._existsinstorage = False

        except AzureMissingResourceHttpError as e:
            log.debug('can not delete table entity:  Table {}, PartitionKey {}, RowKey {} because {!s}'.format(self._tablename, self._PartitionKey, self._RowKey, e))

    def __changeprimarykeys__(self, PartitionKey = '', RowKey = ''):
        """ Change Entity Primary Keys into new instance:

            - PartitionKey and/or
            - RowKey
        """

        PartitionKey = PartitionKey if PartitionKey != '' else self._PartitionKey
        RowKey = RowKey if RowKey != '' else self._RowKey

        """ change Primary Keys if different to existing ones """
        if (PartitionKey != self._PartitionKey) or (RowKey != self._RowKey):
            return True, PartitionKey, RowKey
        else:
            return False, PartitionKey, RowKey
        pass
            
    def moveto(self, PartitionKey = '', RowKey = ''):
        """ Change Entity Primary Keys and move in Storage:

            - PartitionKey and/or
            - RowKey
        """
        changed, PartitionKey, RowKey = self.__changeprimarykeys__(PartitionKey, RowKey)

        if changed:

            """ sync self """
            new = self.copyto(PartitionKey, RowKey)
            new.save()

            """ delete Entity if exists in Storage """
            self.delete()

    def copyto(self, PartitionKey = '', RowKey = '') -> object:
        """ Change Entity Primary Keys and copy to new Instance:

            - PartitionKey and/or
            - RowKey
        """
        changed, PartitionKey, RowKey = self.__changeprimarykeys__(PartitionKey, RowKey)

        self.load()
        new = self
        new._PartitionKey = PartitionKey
        new._RowKey = RowKey
        new.load()

        return new

    pass


class StorageTableEntity(dict):
    _tablename = ''
    _dateformat = ''
    _datetimeformat = ''

    def __init__(self, **kwargs):                  
        """ constructor """
        self._tablename = self.__class__._tablename
        self._dateformat = self.__class__._dateformat
        self._datetimeformat = self.__class__._datetimeformat

                
        """ parse **kwargs into instance var """
        self._PartitionKey = kwargs.get('PartitionKey', '')
        self._RowKey = kwargs.get('RowKey', '')

        for key, default in vars(self.__class__).items():
            if not key.startswith('_') and key != '':
                if key in kwargs:
                   
                    value = kwargs.get(key)
                    to_type = type(default)
                
                    if to_type is StorageTableCollection:
                        setattr(self, key, value)

                    elif to_type is datetime.datetime:
                        setattr(self, key, safe_cast(value, to_type, default, self._datetimeformat))

                    elif to_type is datetime.date:
                        setattr(self, key, safe_cast(value, to_type, default, self._dateformat))

                    else:
                        setattr(self, key, safe_cast(value, to_type, default))
                
                else:
                    setattr(self, key, default)

        """ set primary keys from data"""
        if self._PartitionKey == '':
            self.__setPartitionKey__()

        if self._RowKey == '':
            self.__setRowKey__()

        """ init Storage Table Collections in Instance """
        self.__setcollections__()
        pass
           
    def __setPartitionKey__(self):
        """ parse storage primaries from instance attribute 
            overwrite if inherit this class
        """
        pass

    def __setRowKey__(self):
        """ parse storage primaries from instance attribute 
            overwrite if inherit this class
        """
        pass

    def __setcollections__(self):
        """ initialize StorageTable collections  
            overwrite if inherit this class
        """
        pass

    pass


class StorageTableCollection():
    _tablename = ''
    _tableservice = None
    _entities = None
    _count = None

    def __init__(self, tableservice:TableService, tablename, filter='*'):
        """ constructor """

        """ query configuration """
        self._tableservice = tableservice
        self._tablename = tablename
        self._entities = None
        self._count = None
        self._filter = filter
        pass

    def load(self):
        if not self._tableservice is None:
            log.debug('query table {} filter by {}'.format(self._tablename, self._filter))
            self._entities = self._tableservice.query_entities(self._tablename, self._filter)
            self._count = len(list(self._entities))
            
    def list(self) -> list:
        if self._entities is None:
            self.load()
        return list(self._entities)

    def count(self) -> int:
        if self._count is None:
            self.load()
        return self._count

    def find(self, key, value):
        pass

    def delete(self):
        pass

    pass
