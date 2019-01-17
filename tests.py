# -*- coding: utf-8 -*-

import unittest
import os

from main import *
from connection import Connection

cur_dir = os.path.dirname(os.path.abspath(__file__))


class TestConnection(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestConnection, self).__init__(*args, **kwargs)
        self.query_result = []

    def setUp(self):
        config = ConfigParser.ConfigParser()
        config.read(cur_dir + '/settings/systems.ini')
        self.data = config._sections['SpecialViews']
        self.test_object = Connection(json.loads(self.data['db']))

    def test_special_views_config(self):
        self.assertIsInstance(self.data.get('default_wfs'), str)
        self.assertIsInstance(self.data.get('yarn_root_url'), str)
        self.assertIsInstance(self.data.get('mail_list'), str)
        self.assertIsInstance(self.data.get('mail_list').split('\n'), list)
        self.assertLess(max([len(email) for email in self.data.get('mail_list').split(', ')]), 50)

    def test_running_query(self):
        query_result = self.test_object.get_failed_workflows(self.data.get('default_wfs'), period='24 hours')
        self.assertGreater(len(query_result), 1)
        self.query_result = query_result


class WorkflowTest(unittest.TestCase):

    def setUp(self):
        config = ConfigParser.ConfigParser()
        config.read(cur_dir + '/settings/systems.ini')
        data = config._sections['SpecialViews']
        conn = Connection(json.loads(data['db']))
        test_row = conn.get_failed_workflows(data.get('default_wfs'), period='24 hours')[0]
        self.test_wf = WorkFlow(test_row, 'https://fada25.cloud.df.sbrf.ru:8888/', 0, 0)

    def test_handle_workwlow(self):
        result, logs = self.test_wf.handle_workwlow()
        results = re.sub(r'<td>|</td>|<tr>|</tr>', ' ', result).split()
        self.assertEqual(len(results), 12)
        self.assertIsInstance(int(results[0]), int)


if __name__ == '__main__':
    unittest.main()
    a = tuple()

