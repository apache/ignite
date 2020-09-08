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

from random import randint
import jaydebeapi
from ignitetest.services.ignite import IgniteService
from ignitetest.utils.version import DEV_BRANCH


class SqlClient:
    """
    SQL Client using the JDBC driver.
    """

    # pylint: disable=R0913
    def __init__(self, ignite_service: IgniteService):
        """
        :param ignite_service: IgniteService.
        """
        self.ignite_service = ignite_service
        self.conn = connection(ignite_service)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """
        Close self connection.
        """
        self.conn.close()

    def execute(self, operation, parameters=None):
        """
        Execute.
        """
        with self.conn.cursor() as curs:
            curs.execute(operation, parameters)
            return curs.fetchall()

    def executemany(self, operation, seq_of_parameters):
        """
        Executemany.
        """
        with self.conn.cursor() as curs:
            curs.executemany(operation, seq_of_parameters)
            return curs.rowcount


def connection(ignite_service: IgniteService):
    """
    :param ignite_service: IgniteService.
    :return Connection.
    """
    if ignite_service.config.version == DEV_BRANCH:
        core_jar_path = 'modules/core/target/ignite-core-2.10.0-SNAPSHOT.jar'
    else:
        core_jar_path = str("%s/libs/ignite-core-%s.jar" %
                            (ignite_service.spec.path.home, ignite_service.config.version))

    node = ignite_service.nodes[randint(0, ignite_service.num_nodes - 1)]

    url = "jdbc:ignite:thin://" + node.account.externally_routable_ip

    return jaydebeapi.connect(jclassname='org.apache.ignite.IgniteJdbcThinDriver',
                              url=url,
                              jars=core_jar_path)
