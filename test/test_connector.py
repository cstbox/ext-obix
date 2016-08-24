#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
from collections import namedtuple
import os
import json
import time
from textwrap import dedent
from xml.etree import ElementTree as ET

from pycstbox.evtmgr import EventManager
from pycstbox.log import getLogger
from pycstbox.obix.connector import OBIXConnector


__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'


FakeResponse = namedtuple('FakeResponse', 'status_code text reason')
TracedEvent = namedtuple('TracedEvent', 'var_type var_name data')

TMP_CONFIG_PATH = os.path.join('/tmp', OBIXConnector.DEFAULT_CONFIG_NAME)


class MockUpEventManager(EventManager):
    def __init__(self):
        self.emitted_events = []
        self._log = getLogger('evtMgr')

    def emitEvent(self, var_type, var_name, data):
        self._log.info('emit event (%s, %s, %s)', var_type, var_name, data)
        self.emitted_events.append(TracedEvent(var_type, var_name, data))


class TestOBIXConnector(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cfg = {
            "gateway": {
                "host": "oBIX_gateway_host_name_or_IP",
                "node_id": "oBIX_gateway_nodeID",
                "device_id": "oBIX_gateway_deviceID"
            },
            "mapping": {
                "AV101": ["var_101", "concentration"],
                "AV102": ["var_102", "counter"],
                "AV103": ["var_103", "counter"],
                "AV104": ["var_104", "luminosity"],
            },
            "global": {
                "events_ttl": "2h"
            }
        }
        with open(TMP_CONFIG_PATH, 'w') as fp:
            json.dump(cfg, fp)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(TMP_CONFIG_PATH):
            os.remove(TMP_CONFIG_PATH)

    def setUp(self):
        # loads the content of default reply XML
        doc = ET.parse('fixtures/sample_reply.xml')
        self.reply_items = {
            child.attrib['href'].strip('/').split('/')[-2]: child
            for child in doc.getroot()
        }

    def test_01_reqok(self):
        verbose_gw = False
        queried_sensors = []

        def fake_query_gateway(url, data):
            global queried_sensors

            if verbose_gw:
                print(dedent("""
                gateway simulation:
                url : %(url)s
                data : %(data)s
                """).strip() % {
                    'url': url, 'data': data
                })
            request = ET.fromstring(data)
            queried_sensors = [child.attrib['val'].strip('/').split('/')[-2] for child in request]

            reply = ET.fromstring("""
                <list xmlns:c2g="http://www.can2go.com" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" of="obix:obj"
                      xsi:schemaLocation="http://obix.org/ns/schema/1.0/obix/xsd" href="obix:BatchOut"
                      xmlns="http://obix.org/ns/schema/1.0">
                </list>
            """)
            for s in queried_sensors:
                reply.append(self.reply_items[s])

            return FakeResponse(200, ET.tostring(reply), None)

        em = MockUpEventManager()
        obix = OBIXConnector(TMP_CONFIG_PATH, em, alt_query_gateway=fake_query_gateway)
        obix.step_run(loop_callback=lambda _: False)

        self.assertEquals(len(em.emitted_events), 4)
        for i, sensor in enumerate(queried_sensors):
            self.assertEquals(em.emitted_events[i].var_name, sensor)

    def test_02_reqerr(self):
        def fake_query_gateway(url, data):
            return FakeResponse(404, '', 'not found')

        em = MockUpEventManager()
        obix = OBIXConnector(TMP_CONFIG_PATH, em, alt_query_gateway=fake_query_gateway)
        obix.step_run(loop_callback=lambda _: False)

        self.assertEquals(len(em.emitted_events), 0)


class TestOBIXConnectorReal(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cfg = {
            "gateway": {
                "host": "193.251.52.95",
                "node_id": "N001C42",
                "device_id": "DEV201"
            },
            "mapping": {
                "AV101": ["var_101", "concentration"],
                "AV102": ["var_102", "counter"],
                "AV103": ["var_103", "counter"],
                "AV104": ["var_104", "luminosity"],
            },
            "global": {
                "events_ttl": "2h"
            }
        }
        with open(TMP_CONFIG_PATH, 'w') as fp:
            json.dump(cfg, fp)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(TMP_CONFIG_PATH):
            os.remove(TMP_CONFIG_PATH)

    def test_01(self):
        em = MockUpEventManager()
        obix = OBIXConnector(TMP_CONFIG_PATH, em)
        obix.step_run(loop_callback=lambda _: False)
        self.assertEquals(len(em.emitted_events), 4)

    def test_02(self):
        em = MockUpEventManager()
        obix = OBIXConnector(TMP_CONFIG_PATH, em)

        MAX_LOOPS = 3
        self.loop_cnt = 0

        def callback(connector):
            self.loop_cnt += 1
            connector.log_info('loop %d done', self.loop_cnt)

            if self.loop_cnt == MAX_LOOPS:
                return False

            time.sleep(3)
            return True

        obix.step_run(loop_callback=callback)

        # only 4 events should have been sent since the values do not change
        self.assertEquals(len(em.emitted_events), 4)

if __name__ == '__main__':
    unittest.main()
