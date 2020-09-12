# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This module contains JDBC driver wrapper.
"""

import random
import jaydebeapi
from ignitetest.services.ignite import IgniteService
from ignitetest.utils.version import DEV_BRANCH, IgniteVersion


def jdbc_connection(ignite_service: IgniteService, ver: IgniteVersion = None):
    """
    :param ignite_service: IgniteService.
    :param ver: IgniteVersion jdbc driver for connection.
    :return Connection.
    """
    if ver is None:
        ver = ignite_service.config.version

    vstr = ver.vstring

    if ver == DEV_BRANCH:
        core_jar_path = str("modules/core/target/ignite-core-%s-SNAPSHOT.jar" % vstr)
    else:
        core_jar_path = str("/opt/ignite-%s/libs/ignite-core-%s.jar" % (vstr, vstr))

    node = random.choice(ignite_service.nodes)

    url = "jdbc:ignite:thin://" + node.account.externally_routable_ip+"/?distributedJoins=true"

    return jaydebeapi.connect(jclassname='org.apache.ignite.IgniteJdbcThinDriver',
                              url=url,
                              jars=core_jar_path)
