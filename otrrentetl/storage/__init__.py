""" Initialize azure storage repository """

from config import config

from storage.azurestoragewrapper import StorageContext
db = StorageContext(**config)




