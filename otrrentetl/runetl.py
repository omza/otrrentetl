""" imports & globals """
from datetime import datetime, timedelta
import os, fnmatch
from sys import stderr, stdout, stdin

import logging
import logging.handlers

import signal

from server.etl import import_otrepg, import_otrgenres, update_toprecordings, update_torrents
from server.helper import safe_cast
from config import config, log


stopsignal = False

def handler_stop_signals(signum, frame):
    global stopsignal
    stopsignal = True

signal.signal(signal.SIGINT, handler_stop_signals)
signal.signal(signal.SIGTERM, handler_stop_signals)


""" Main """
def main():
    

    nextrun =  datetime.utcnow()
    log.info('otrserver start main....')
    
    """ log configuration in debug mode """
    if config['APPLICATION_LOG_LEVEL'] == 'DEBUG':
        for key, value in config.items():   
            log.debug('otrrentserver configuration: {} = {!s}'.format(key, value))

    """ run until stopsignal """
    while not stopsignal:

        if (datetime.utcnow() >= nextrun):

            """ run etl  """ 
            log.info('run ETL')

            genres = import_otrgenres()

            """ loop back for 10 days and import"""
            iterdate = datetime.now().date() - timedelta(days=10)
            startdate = datetime.now().date() - timedelta(days=8)
            enddate = datetime.now().date() - timedelta(days=1 )
            while (iterdate <= enddate):             
                if (iterdate < startdate):
                    """ housekeeping(iterdate) """
                else:
                    import_otrepg(iterdate, genres)
                    pass

                iterdate = iterdate + timedelta(days=1)

            update_toprecordings()

            update_torrents(startdate)
            
            nextrun = datetime.utcnow() + timedelta(seconds=config['APPLICATION_ETL_INTERVAL'])
            log.info('next runtime ETL in {!s} seconds at {!s}'.format(config['APPLICATION_ETL_INTERVAL'], nextrun))

    """ goodby """ 
    log.info('otrrentserver main terminated. Goodby!')

""" run main if not imported """
if __name__ == '__main__':
    main()
 