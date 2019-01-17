# -*- coding: utf-8 -*-

import psycopg2
import logging
logger = logging.getLogger('__main__.connection')

from queries import find_failed_workflows_query
from settings.settings import period


class Connection(object):
    def __init__(self, db_params):
        try:
            self.connection = psycopg2.connect(**db_params)
            self.cursor = self.connection.cursor()
        except Exception as e:
            logger.critical("Cannot conect to PSQL Database")
            logger.debug(str(e))
        else:
            logger.debug("Successfully connected to CTL DB")

    def __del__(self):
        self.connection.close()
    
    def get_failed_workflows(self, wfs, period=period):
        """
        return: результат выполнения запроса на поиск упавших потоков
        """
        try:
            logger.debug("Trying to execute find_workflows query")
            self.cursor.execute(find_failed_workflows_query % (wfs, period))
        except Exception as e:
            logger.critical("Cannot execute find_workflows query")
            logger.debug(str(e))
            logger.debug(find_failed_workflows_query % (wfs, period))
            return []
        else:
            logger.debug("find_workflows query was successfully executed")
            return self.cursor.fetchall()
        
    def close(self):
        """закрываем коннект к базе"""
        self.cursor.close()
        self.connection.close()
