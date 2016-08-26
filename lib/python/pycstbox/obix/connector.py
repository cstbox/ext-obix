# -*- coding: utf-8 -*-

import json
from collections import namedtuple
from threading import Thread
import time
import xml.etree.ElementTree as ET
import re

import requests

from pycstbox import log
from pycstbox.events import make_data
from pycstbox.sysutils import parse_period

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'


GatewayConfiguration = namedtuple('GatewayConfiguration', 'host node_id device_id')


class OBIXConnector(log.Loggable):
    """ OBIX gateway connector.

    This connector is responsible for retrieving periodically the sensor values collected by an OBIX gateway
    (e.g. Can2Go), and publishing the corresponding sensor events, following the same rules as standard
    sensor drivers.

    The net result is to have Can2Go managed sensors as if they were directly connected to the CSTBox.

    The dialog with the OBIX gateway uses its REST API.

    The connector is configured by a JSON file, containing a dictionary structured as follows ::

        {
            "gateway": {                    # gateway settings
                "host": ... ,               # host name or IP
                "node_id": ...,             # node id as defined in the gateway
                "device_id": ...            # device id as defined in the gateway
            },
            "mapping": {                    # mapping between OBIX variables and CSTBox ones
                <obix_var_name>: [<CSTBox_var_name>, <CSTBox_var_type>],
                ...
            },
            "global": {
                "events_ttl": ...           # max age of events (ex: 2h, 60m,...). default: 2h
            }
        }
    """
    DEFAULT_CONFIG_NAME = 'cstbox-obix.cfg'

    OBIX_URL_TEMPLATE = "http://%(host)s/obix/batch"
    OBIX_REQUEST_PROLOG = '<?xml version="1.0" encoding="UTF-8"?><list is="obix:BatchIn">'
    OBIX_BATCH_ITEM_TEMPLATE = \
        '<uri is="obix:Read" ' \
        'val="http://%(host)s/obix/network/%(node_id)s/%(device_id)s/%(sensor_id)s/Present_Value/"/>'
    OBIX_REQUEST_EPILOG = '</list>'

    DEFAULT_POLLING_PERIOD = 5 * 60         # 5 minutes
    ERROR_REPORT_TTL = 2 * 3600             # 2 hours
    SOLID_FAILURE_THRESHOLD = 24 * 3600     # 24 hours

    DEFAULT_EVENTS_TTL = 2 * 3600           # 2 hours

    def __init__(self, config_path, evt_mgr, log_level=log.INFO, alt_query_gateway=None):
        """
        :param str config_path: path of the configuration file
        :param evt_mgr: CSTBox event manager proxy object
        :param log_level: logging level
        :param alt_query_gateway: an alternate version of :py:meth:`_query_gateway` for unit tests
        """
        log.Loggable.__init__(self)
        self.log_setLevel(log_level)

        log.getLogger('requests').setLevel(log.INFO if self.logger.isEnabledFor(log.DEBUG) else log.WARN)

        if not evt_mgr:
            raise ValueError('evt_mgr parameter is mandatory')
        self._evt_mgr = evt_mgr

        with open(config_path) as fp:
            self.log_info("loading configuration from %s", config_path)
            cfg_dict = json.load(fp)

        gw_cfg = cfg_dict['gateway']
        self.log_info("gateway configuration :")
        for k, v in gw_cfg.iteritems():
            self.log_info("- %-20s : %s", k, v)
        self._gateway_cfg = GatewayConfiguration(**gw_cfg)

        # loads the {obix_id: (var_name, var_type)} mapping
        self._mapping = cfg_dict['mapping']
        self.log_info("mapping configuration :")
        for k, v in self._mapping.iteritems():
            self.log_info("- %-20s -> %s", k, v)

        # creates the reverse lookup {var_name: obix_id}
        self._reverse_mapping = {var_def[0]: obix_id for obix_id, var_def in self._mapping.iteritems()}

        self._worker_thread = None
        self._terminate = False

        self.log_info("global configuration :")
        try:
            global_cfg = cfg_dict["global"]
        except KeyError:
            self._events_ttl = self.DEFAULT_EVENTS_TTL
            self._polling_period = self.DEFAULT_POLLING_PERIOD
        else:
            try:
                self._events_ttl = parse_period(global_cfg['events_ttl'])
            except KeyError:
                self._events_ttl = self.DEFAULT_EVENTS_TTL

            try:
                self._polling_period = parse_period(global_cfg['polling_period'])
            except KeyError:
                self._polling_period = self.DEFAULT_POLLING_PERIOD
        for k, v in (("events_ttl", self._events_ttl), ("polling_period", self._polling_period)):
            self.log_info("- %-20s : %s", k, v)

        # patch the default gateway query process by the provided one if any
        if alt_query_gateway:
            self._query_gateway = alt_query_gateway

    def start(self):
        if self._worker_thread:
            self.log_warn('start ignored : already running')
            return

        self._worker_thread = Thread(
            name=self.__class__.__name__ + '.worker',
            target=self._polling_loop,
            kwargs={
                'url': self.OBIX_URL_TEMPLATE % {'host': self._gateway_cfg.host}
            }
        )
        self._terminate = False
        self._worker_thread.start()

    def step_run(self, loop_callback):
        """ Step by step pass in loop for unit tests.

        .. important:: NOT FOR NORMAL USE

        :param callable loop_callback: called at the end of the loop to check if it must be continued or not
        """
        self._polling_loop(url=self.OBIX_URL_TEMPLATE % {'host': self._gateway_cfg.host}, loop_callback=loop_callback)

    def _query_gateway(self, url, data):
        """ Wraps real gateway query, so that the real process can be replaced by simulated on
        in unit tests context.
        """
        return requests.post(url, data=data)

    def _polling_loop(self, url=None, loop_callback=None):
        logger = self.logger.getChild('worker')

        sensor_list = self._mapping.keys()
        batch_items = [self.OBIX_BATCH_ITEM_TEMPLATE % {
            'host': self._gateway_cfg.host,
            'node_id': self._gateway_cfg.node_id,
            'device_id': self._gateway_cfg.device_id,
            'sensor_id': obix_sensor
        } for obix_sensor in sensor_list]
        obix_request = ''.join([self.OBIX_REQUEST_PROLOG] + batch_items + [self.OBIX_REQUEST_EPILOG])

        # OBIX to Python types mapping
        pythonize = {
            'bool': bool,
            'int': int,
            'real': float
        }

        last_values = {}
        reported_errors = {}

        next_schedule = 0

        ns_sub_pattern = re.compile(r'{.*}')

        request_exception_level = 0
        logger.info('starting polling loop')
        while not self._terminate:
            now = time.time()

            if now >= next_schedule:

                try:
                    reply = self._query_gateway(url, data=obix_request)

                except requests.RequestException as e:
                    if request_exception_level == 0:
                        logger.error('gateway request error : %s', e)
                        request_exception_level = 1
                    elif request_exception_level == 1:
                        logger.error('gateway request error (solid) : %s', e)
                        request_exception_level = 2
                    else:
                        pass    # don't report anymore

                else:
                    if request_exception_level:
                        logger.info('recovered from gateway request error')
                        request_exception_level = 0

                    if reply.status_code == 200:
                        root = ET.fromstring(reply.text)
                        for child, obix_sensor in zip(root, sensor_list):
                            tag = child.tag
                            if tag != 'err':
                                # strip namespaces for simplification's sake (we don't support them for the moment)
                                tag = ns_sub_pattern.sub('', tag)

                                # convert the string representation of the value in a typed one, using
                                # the tag-to-datatype mapping
                                value = pythonize[tag](child.attrib['val'])

                                # get the unit if any
                                try:
                                    unit = child.attrib['unit'].split('/')[-1]
                                except KeyError:
                                    unit = None

                                # get the CSTBox var type and name, based on the OBIX-to-CSTBox mapping
                                # specified in the configuration file
                                var_name, var_type = self._mapping[obix_sensor]

                                # has the value changed from last time ? If yes, publish the corresponding event
                                # and remember the new one
                                last_value, mtime = last_values.get(var_name, (None, None))
                                if value != last_value or now - mtime >= self._events_ttl:
                                    self._evt_mgr.emitEvent(
                                        var_type, var_name, json.dumps(make_data(value, units=unit))
                                    )
                                    last_values[var_name] = (value, now)

                                # clear any existing error condition for this sensor
                                try:
                                    del reported_errors[obix_sensor]
                                except KeyError:
                                    pass

                            else:
                                # we got and error report :(
                                # Report it, but in a smart way in case this error is repeated.
                                # We use a memory of past errors and a two levels error report time to live (TTL)
                                # to determine what must be done.
                                last_report, first_report = reported_errors.get(obix_sensor, (0, now))

                                if now - first_report <= self.SOLID_FAILURE_THRESHOLD:
                                    if now - last_report >= self.ERROR_REPORT_TTL:
                                        try:
                                            msg = child.attrib['display']
                                        except KeyError:
                                            msg = child.attrib['is'].split(':')[-1]
                                        self.log_error('%s read request error : %s', obix_sensor, msg)
                                        reported_errors[obix_sensor] = (now, first_report)

                                else:   # solid error => notify it less frequently
                                    if now - last_report >= self.SOLID_FAILURE_THRESHOLD:
                                        self.log_error('solid error for sensor %s', obix_sensor)
                                        reported_errors[obix_sensor] = (now, first_report)

                    else:   # gateway request gave an error
                        self.log_error('gateway request failure : (%d) %s', reply.status_code, reply.reason)

                next_schedule = now + self._polling_period

            if loop_callback and not loop_callback(self):
                break
            else:
                # check terminate often enough for optimal reactivity
                time.sleep(0.5)

        logger.info('polling loop terminated')

    def terminate(self):
        if self._worker_thread:
            self.log_info('terminating polling thread...')
            self._terminate = True
            self._worker_thread.join(30)
            self._worker_thread = None
            self.log_info('complete.')

    def __del__(self):
        self.terminate()


class OBIXConnectorError(Exception):
    pass
