""" imports & globals """
import os
from sys import stderr, stdout, stdin

import logging
import logging.handlers

from server.helper import safe_cast

""" Main configuration """
config = {}

""" import configuration depending on environment """
config['APPLICATION_ENVIRONMENT'] = safe_cast(os.environ.get('APPLICATION_ENVIRONMENT'),str, 'Production')

if config['APPLICATION_ENVIRONMENT'] == 'Development':
    from config.application import dev as configs
elif config['APPLICATION_ENVIRONMENT'] == 'Test':
    from config.application import test as configs
elif config['APPLICATION_ENVIRONMENT'] == 'Production':
    from config.application import prod as configs
else:
    from config.application import prod as configs

tmpconfig = {}
for (key, value) in vars(configs).items():
    if key != '' and key[:2] != '__':
        tmpconfig[key] = value

config.update(tmpconfig)

""" import secret configuration depending if module secrets exists """
try:
    from config.secrets import secrets as secretconfigs

except Exception:
    from config.application import secrets as secretconfigs

finally:
    tmpconfig = {}
    for (key, value) in vars(secretconfigs).items():
        if key != '' and key[:2] != '__':
            tmpconfig[key] = value

    config.update(tmpconfig)



""" Logging Configuration """
log = logging.getLogger(config['APPLICATION_MAINLOGGER'])

formatter = logging.Formatter('%(asctime)s | %(name)s:%(lineno)d | %(funcName)s | %(levelname)s | %(message)s')

consolehandler = logging.StreamHandler(stdout)
consolehandler.setFormatter(formatter)
consolehandler.setLevel(config['APPLICATION_LOGLEVEL_CONSOLE'])
    
logfilename = os.path.join(config['APPLICATION_PATH_LOG'], 'otrrentserver.log')
filehandler = logging.handlers.RotatingFileHandler(logfilename, 10240, 5)
filehandler.setFormatter(formatter)
filehandler.setLevel(config['APPLICATION_LOGLEVEL_FILE'])

log.setLevel(config['APPLICATION_LOGLEVEL_CONSOLE'])
log.addHandler(consolehandler)
log.addHandler(filehandler)





