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
from ignitetest.utils.version import DEV_BRANCH


def connection(ignite_service: IgniteService):
    """
    :param ignite_service: IgniteService.
    :return Connection.
    """
    if ignite_service.config.version == DEV_BRANCH:
        core_jar_path = str("modules/core/target/ignite-core-%s-SNAPSHOT.jar" % DEV_BRANCH.vstring)
    else:
        core_jar_path = str("%s/libs/ignite-core-%s.jar" %
                            (ignite_service.spec.path.home, ignite_service.config.version))

    node = random.choice(ignite_service.nodes)

    url = "jdbc:ignite:thin://" + node.account.externally_routable_ip

    return jaydebeapi.connect(jclassname='org.apache.ignite.IgniteJdbcThinDriver',
                              url=url,
                              jars=core_jar_path)
