# -*- coding: utf-8 -*-

import logging
logger = logging.getLogger("__main__.workflow")

import subprocess
from zipfile import ZipFile, ZIP_DEFLATED
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import os
import xml.etree.ElementTree as ET
import uuid
from datetime import datetime, timedelta
import shutil
import time

from settings.settings import hue_login, hue_pass

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

cur_dir = os.path.dirname(os.path.abspath(__file__))
log_dir = cur_dir + '/logs/'
in_xml_dir = '/home/od99usr/mq_od/in/'
out_xml_dir = '/home/od99usr/mq_od/out/'


class WorkFlow(object):
    def __init__(self, query_result, yarn_root_url, create_incident=False, make_hive_request=False):
        """
        :param query_result: result of executing find failed workflow query
        :param yarn_root_url: yarn root url
        :param create_incident: bool, True if necessary to create incident
        :param make_hive_request: bool, True if necessary make hive request
        """
        self.loading_id = query_result[0]
        self.wf_name = query_result[5]
        self.status = query_result[2]
        self.wf_type = query_result[6]
        self.xid = query_result[3]
        self.change_status_at = query_result[4]
        self.queue_name = query_result[15]
        self.start_at = str(query_result[12])
        self.is_sheduled = str(query_result[13])
        self.category = str(query_result[7])
        self.wf_id = str(query_result[14])
        self.log_name = ''
        self.key_error = None
        self.app_id = ''
        self.username = ''
        self.incident = ''
        self.rquid = uuid.uuid4().hex
        self.create_incident = bool(int(create_incident))
        self.make_hive_request = bool(int(make_hive_request))
        self.yarn_root_url = yarn_root_url
        self.hue_session = None

    def __del__(self):
        if self.hue_session:
            self.hue_session.close()

    def handle_workwlow(self):
        """
        Main public method for getting list of log names and formatted row to paste into final report
        :return: tuple where first element is row for table, second is logname
        """
        if not self.xid:
            logger.info(self.wf_name + " - no XID; return only table line")
            return self.__generate_table_tds(), None
        logs = []
        if self.wf_type == 'oozie':
            logs = self.__get_oozie_log()
            if not logs:
                logger.info(self.wf_name + " - cant get oozie logs; return only table line")
                return self.__generate_table_tds(), None
            yarn_logs = self.__get_yarn_logs(logs)
            if yarn_logs:
                logs.append(yarn_logs)
        else:
            infa_log = self.__get_infa_log_by_xid()
            if infa_log:
                logs.append(infa_log)
            if not logs:
                logger.info(self.wf_name + " - cant get informatica logs; return only table line")
                return self.__generate_table_tds(), None
        if logs:
            try:
                self.key_error = self.__find_all_exceptions(logs)
            except Exception as e:
                logger.error('Cant find exception in logs')
                logger.debug(str(e))
        if self.make_hive_request:
            hive_log = self.__make_hive_request_xstream()
            if hive_log:
                logs.append(hive_log)
        if self.create_incident:
            self.__create_incident()
        logger.info(self.wf_name + ' - find %d logs; return table line and packed logs' % len(logs))
        return self.__generate_table_tds(), self.__pack_logs(logs)

    def __get_oozie_log(self):
        """
        return: list of oozie job logs from HUE
        """
        login_form_data = {
        'csrfmiddlewaretoken': '',
        'username': hue_login,
        'password': hue_pass,
        'next': '/'
        }
        login_url = self.yarn_root_url + 'accounts/login/'
        self.hue_session = requests.Session()
        try:
            r = self.hue_session.get(login_url, verify=False)
        except requests.exceptions.ConnectionError:
            logger.debug('Connection Error')
            return
        try:
            login_form_data['csrfmiddlewaretoken'] = self.hue_session.cookies['csrftoken']
        except KeyError:
            return
        r = self.hue_session.post(login_url, data=login_form_data, cookies=dict(), headers=dict(Referer=login_url))
        try:
            r = self.hue_session.get(self.yarn_root_url + 'oozie/get_oozie_job_log/' + str(self.xid)).json()
        except ValueError, TypeError:
            return
        else:
            try:
                # Get main workflow log
                log = r['log']
                log_name = self.__create_log_by_filename('WORKFLOW' + self.xid, log)
            except KeyError:
                return
        logs = self.__get_oozie_job_log()
        logger.debug("FIND %d LOGS" % len(logs))
        logs.append(log_name)
        return logs

    def __get_oozie_job_log(self, ext_url=None):
        """
        recursive method for find all error job logs

        ext_url - url to get job log
        return: list of oozie job logs
        """
        logs = [] 
        logger.debug("INTO get_oozie_job_log")
        # get page by xid or by ext_url
        try:
            if ext_url:
                logger.debug("ext_url = " + ext_url)
                wf_json = self.hue_session.get(self.yarn_root_url[0:-1] + ext_url + "?format=json").json()
            else:
                logger.debug("XID = " + self.xid)
                wf_json = self.hue_session.get(self.yarn_root_url + "oozie/list_oozie_workflow/" +
                                               self.xid + "/?format=json").json()
        except Exception as e:
            logger.critical("cant' get wf json. See exception bellow")
            logger.debug(str(e))
            return logs
        # Try to get info from actions tab
        else:
            try:
                wf_actions = wf_json['actions']
            except KeyError:
                logger.debug(str(wf_json))
                logger.critical("Can't get actions in json")
                return logs
            for action in wf_actions:
                if action['status'] == 'ERROR' or action['status'] == 'KILLED':
                    # subworkflow has log value null
                    if action['log']:
                        job_id = action['externalId']
                        job_json = self.hue_session.get(self.yarn_root_url[0:-1] + action['log'] + "?format=json").json()
                        try:
                            final_log = job_json['logs']
                        except KeyError:
                            logger.error("Can't get log from json")
                            return logs
                        i = 0
                        for log_type in ('DIAGNOSTIC_', 'STDOUT_', 'STDERR_', 'SYSLOG_'):
                            logname = self.__create_log_by_filename(log_type + job_id, final_log[i])
                            i += 1
                            logs.append(logname)
                    elif not action['log'] and action['externalIdUrl']:
                        # if logs is null it is subworkflow, run method recursively
                        logs += self.__get_oozie_job_log(ext_url=action['externalIdUrl'])
                    else:
                        # if not link and not logs return that have
                        return logs
            return logs

    def __get_infa_log_by_xid(self):
        """
        get informarica log from ETL server (fada27)

        :return log filename
        """
        process = subprocess.Popen('sh find_inf_log.sh %s %s' % (self.xid, log_dir + self.__log_filename),
                                   shell=True, stdout=subprocess.PIPE) 
        process.wait()
        return log_dir + self.__log_filename if process.returncode == 0 else False

    @property
    def __log_filename(self):
        """
        property that return regular logname
        """
        return self.xid + '_' + str(self.status) + '.log'

    def __generate_table_tds(self):
        """
        :return table line for report
        """
        return '<tr><td>' + str(self.wf_id) + '</td><td>' + str(self.category) + \
               '</td><td>' + str(self.loading_id) + '</td><td>' + str(self.wf_name) + \
               '</td><td>' + str(self.xid) + '</td><td>' + str(self.status) + \
               '</td><td>' + str(self.start_at) + '</td><td>' + str(self.is_sheduled) + \
               '</td><td>' + str(self.wf_type) + '</td><td>' + str(self.queue_name) + \
               '</td><td>' + str(self.key_error) + '</td><td>' + self.incident.encode('utf-8') + '</td></tr>'
    
    def __pack_logs(self, logs):
        """
        :param logs: list of ligs to pack
        :return: name of ziped log
        """
        with ZipFile(log_dir + self.__log_filename + '.zip', 'w', ZIP_DEFLATED) as oozie_zip:
            for log in logs:
                if os.path.isdir(log):
                    for dir_log in map(lambda x: log + '/' + x, os.listdir(log)):
                        oozie_zip.write(dir_log)        
                    shutil.rmtree(log, ignore_errors=True)
                    continue
                oozie_zip.write(log)
                os.remove(log)
        return log_dir + self.__log_filename + '.zip'
    
    def __create_incident_xml(self):
        """
        method for create xml to create incident
        :return: xml text
        """
        xml_output = cur_dir + '/xml/' + self.wf_name + '_' + datetime.now().strftime('%Y-%m-%dT%H:%M') + '.xml'
        xml_template = cur_dir + '/xml/template.xml'
        tree = ET.parse(xml_template)
        root = tree.getroot()
        source_id = root.find('Incident').find('SourceID')
        source_id.text = '35ce1d14c31547d8a'
        description = root.find('Incident').find('Description')
        description.text = self.key_error if self.key_error else unicode('Ошибка потока ', 'cp1251') + self.wf_name
        subject = root.find('Incident').find('Title')
        subject.text = unicode('Падение потока', 'cp1251') + self.wf_name
        attach = root.find('Incident').find('AdditionalFields').find('LinkToLogs')
        attach.text = self.log_name
        check_time = root.find('Incident').find('NextBreach')
        check_time.text = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
        tree.write(xml_output, encoding='UTF-8', xml_declaration=True)
        with open(xml_output, 'r') as x:
            return x.read()
    
    @staticmethod
    def __create_log_by_filename(filename, log):
        """
        :param filename: name of log file
        :param log: log text
        :return: full log name
        """
        full_logname = log_dir + filename + '.log'
        with open(full_logname, 'w') as logfile:
            logfile.write(log.encode('utf-8'))
        return full_logname
    
    @staticmethod
    def __find_all_exceptions(logs):
        """
        Method for finding all exceptions in log files

        :param logs: log files
        :return: exceptions or empty string
        """
        exceptions = []
        for log in logs:
            with open(log, 'r') as f:
                for line in f.readlines():
                    if line.lstrip().startswith("Exception"):
                       exceptions.append(line.lstrip())
        return '\n'.join(set(exceptions)) if exceptions != [] else None
        
    def __make_hive_request_xstream(self):
        """
        :return: name of log with results of hive request
        """
        source_name = self.wf_name.split('_')[0]
        loading_id = str(self.loading_id)
        with open('xstream_hive_template.hql', 'r') as template:
            with open('hive_tmp.hql', 'w') as run_file:
                run_file.write(template.read().replace('${SOURCE_NAME}', source_name).replace('${CTL_LOADING}',
                                                                                              loading_id))
        process = subprocess.Popen('sh hive.sh %s %s' % (source_name, loading_id), shell=True, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        process.wait()
        result = process.communicate()[0]
        os.remove('hive_tmp.hql')
        if process.returncode != 0:
            return None
        else:
            logname = self.__create_log_by_filename('hive_log', result)
            return logname
        
    def __get_app_id_and_username(self, logs):
        """
        :param logs: names of log files
        :return: True if app_id and username were executed from logs, else False
        """
        for log in logs:
            if log.split('/')[-1].startswith('STDOUT'):
                with open(log, 'r') as l:
                    for line in l.readlines():
                        if line.lstrip().lower().startswith('files in current dir'):
                            try:
                                self.username = line.split(':')[1].split('/')[5]
                                self.app_id = line.split(':')[1].split('/')[7]
                                logger.debug(self.username)
                                logger.debug(self.app_id)
                            except IndexError:
                                logger.debug(line)
                                return False
                            else:
                                return True
                                
    def __get_yarn_logs(self, logs):
        """
        method for getting yarn logs from hadoop

        :param logs: names of log files
        :return: yarn logs filename or None
        """
        if self.__get_app_id_and_username(logs):
            process = subprocess.Popen('sh get_yarn_logs.sh %s %s %s' % (self.username, self.app_id, log_dir),
                                       shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            process.wait()
            if process.returncode != 0:
                return None
            else:
                return log_dir + self.app_id
        else:
            return None
    
    def __put_xml(self, message, method):
        """
        method for putting xml into in directory of mq-od integration
        :param message: message to put
        :param method: method to put
        :return: None
        """
        with open(in_xml_dir + method + '_' + self.rquid + '.xml', 'w') as f:
            f.write(message)
    
    def __get_xml(self, method):
        """
        method for getting xml in "out" directory of mq-od integration

        :param method: method to get
        :return: xml text
        """
        timeout = time.time() + 30 # seconds
        while True:
            if time.time() > timeout:
                logger.error('stoped by timeout')
                return None
            response = [f for f in os.listdir(out_xml_dir) if f == method + '_' + self.rquid + '.xml']
            if response:
                with open(out_xml_dir + response[0]) as f:
                    xml_text = f.read()
                    os.remove(out_xml_dir + response[0])
                return xml_text
            
    def __create_incident(self):
        """
        method for creation incident

        :return: None
        """
        self.__put_xml(self.__create_incident_xml(), 'PutIncidentRq')
        response = self.__get_xml('PutIncidentRs')
        if response:
            body_tree = ET.fromstring(response)
            incident_id = body_tree.find('Incident').find('IncidentID')
            state = body_tree.find('Status').find('ServerStatusCode')
            state_desc = body_tree.find('Status').find('StatusDesc')
            if state.text != 'SUCCESS':
                self.incident = state_desc.text
                return None
            self.incident = incident_id.text
            return None
