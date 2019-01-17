# -*- coding: utf-8 -*-

import logging
from logging.handlers import RotatingFileHandler
import requests
from BeautifulSoup import BeautifulSoup
import argparse
import ConfigParser
import json
import re
from multiprocessing import Pool
from contextlib import closing
from functools import partial

from connection import Connection
from workflow import WorkFlow, cur_dir, log_dir

from settings.settings import confluence_login_data, confluence_login_url
from sendmail import table_template, send_mes


LOGLEVEL = logging.INFO


class MyLogger(logging.Logger):
    def makeRecord(self, name, level, fn, lno, msg, args, exc_info,
                   func=None, extra=None):
        new_message = ''
        for symbol in msg:
            if ord(symbol) < 128:
                new_message += symbol
            else:
                pass
        return super(MyLogger, self).makeRecord(name, level, fn, lno, new_message, args, exc_info,
                                                func=None, extra=None)


def get_logger(name):
    """
    Create logger 
    """    
    logging.setLoggerClass(MyLogger)
    logger = logging.getLogger(__name__)
    logger.setLevel(LOGLEVEL)
    handler = RotatingFileHandler(cur_dir + '/running_logs/' + name + '.log', maxBytes=5000000, backupCount=10)
    handler.setLevel(LOGLEVEL)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    return logger
        
        
def get_wfs_from_confluence(wf_page):
    if not wf_page:
        return
    wfs = []
    session = requests.Session()
    try:
        session.post(confluence_login_url, data=confluence_login_data, cookies=dict(), headers=dict(Referer=confluence_login_url), verify=False)
    except Exception as e:
        logger.error('Cant get info from confluence')
        logger.debug(str(e))
        return None
    else:
        r = session.get(wf_page)
        page = r.text
        soup = BeautifulSoup(page)
        rows = soup.findAll('tr')
        for row in rows:
            try:
                cells = row.findAll('td')
                wfs.append(cells[1].text)
            except IndexError:
                pass
        wfs_quoted = map(lambda x: "'" + x + "'", wfs)
        return ','.join(wfs_quoted)


def get_script_params():
    parser = argparse.ArgumentParser()
    parser.add_argument("-system", help="Name of system to check. You can find names of systems in settings/systems.ini file. It is names of sections.", default='prom')
    parser.add_argument("-period", help="Period to check failed wfs.", default='5 minutes')
    parser.add_argument("--test", help="test mode. send mail to avramenko-da", action='store_true', default=False)
    args = parser.parse_args()
    config = ConfigParser.ConfigParser()
    config.read(cur_dir + '/settings/systems.ini')
    if not config.has_section(args.system):
        raise Exception('No system was found in systems.ini')
    data = config._sections[args.system]
    data['system'] = args.system
    if not data.get('db') or not (data.get('default_wfs') or data.get('workflows_page')) or not data.get('yarn_root_url') or not data.get('mail_list'):
        raise Exception('One of required params in section %s is empty or missfound. Check it!')
    wfs = get_wfs_from_confluence(data.get('workflows_page'))
    if not wfs and not data.get('default_wfs'):
        logger.critical(data['system'] + ' - No workflows to check!')
        raise Exception('No workflows to check!')
    elif not wfs and data.get('default_wfs'):
        logger.debug(data['system'] + '- Get workflows list from config')
        data['wfs'] = re.sub(r'\n', '', data.get('default_wfs'))
    else:
        data['wfs'] = wfs
    if args.test:
        data['mail_list'] = 'avramenko-da@mail.ca.sbrf.ru'
    data['period'] = args.period
    return data


def get_logs(wf_data, yarn_root_url='', create_incident=False, make_hive_request=False):
    workflow_to_handle = WorkFlow(wf_data, yarn_root_url, create_incident, make_hive_request)
    return workflow_to_handle.handle_workwlow()


def change_logger_logname(logger_to_change, new_name):
    logger_to_change.removeHandler(logger_to_change.handlers[0])
    handler = RotatingFileHandler(cur_dir + '/running_logs/' + new_name + '.log', maxBytes=5000000, backupCount=10)
    handler.setLevel(LOGLEVEL)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


if __name__ == '__main__':
    logger = get_logger('before_gettinng_params_log')
    data = get_script_params()
    change_logger_logname(logger, data['__name__'])
    conn = Connection(json.loads(data['db'])) # Подлючаемся к БД
    failed_wfs = conn.get_failed_workflows(wfs=data['wfs'], period=data['period'])
    cells = '' # Строки в таблице, которая будет отправлена в письме
    logs = [] # Имена лог-файлов для приаттачивания к письму
    # Если найдены упавшие потоки
    if failed_wfs:
        logger.debug("Find workflows:" + str(failed_wfs))
        # То для каждой строки в результате
        results = map(partial(get_logs, yarn_root_url=data.get('yarn_root_url'),
                                       create_incident=data.get('create_incident'),
                                       make_hive_request=data.get('make_hive_request')
                                       ),
                               failed_wfs)
        for result in results:
            cells += result[0]
            if result[1]:
                logs.append(result[1])
        logger.debug("STARTING TO SEND LOGS: " + str(logs))
        subj_prefix = '[' + data.get('system') + ']'
        subj_continuation = failed_wfs[0][5] + '_' + failed_wfs[0][2] if len(logs) == 1 else 'CTL check'
        send_mes(table_template % cells, data.get('mail_list').split(', '), logs, subj=subj_prefix + subj_continuation)
    else:
        logger.info("No failed loadings were found")
