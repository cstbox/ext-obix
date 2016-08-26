#!/usr/bin/env python
# -*- coding: utf-8 -*-

# This file is part of CSTBox.
#
# CSTBox is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# CSTBox is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with CSTBox.  If not, see <http://www.gnu.org/licenses/>.

""" oBIX data mirroring daemon.

This services polls the oBIX server to extract present values of the sensors
mapped to configured variables of interest.
"""

import sys

from pycstbox import cli
from pycstbox import log 
from pycstbox import dbuslib 
from pycstbox import evtmgr

from pycstbox.obix import OBIXConnector, OBIXConnectorError

__author__ = 'Eric PASCUAL - CSTB (eric.pascual@cstb.fr)'


if __name__ == '__main__':
    parser = cli.get_argument_parser(description="oBIX connector daemon")
    cli.add_config_file_option_to_parser(parser, dflt_name=OBIXConnector.DEFAULT_CONFIG_NAME)

    args = parser.parse_args()
    log_level = getattr(log, args.loglevel)

    dbuslib.dbus_init()
    em = evtmgr.get_object(evtmgr.SENSOR_EVENT_CHANNEL)

    try:
        polling_daemon = OBIXConnector(args.config_path, em, log_level)
        try:
            polling_daemon.start()
        except Exception as e:
            sys.exit(1)

    except OBIXConnectorError as e:
        sys.exit(1)
