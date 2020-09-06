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
import jaydebeapi


# pylint: disable=W0223
class SqlUtil:
    """
    SQL util using the JDBC driver.
    """

    # pylint: disable=R0913
    def __init__(self, url: str,
                 jar_path: str,
                 jdbc_driver_name: str = 'org.apache.ignite.IgniteJdbcThinDriver'):
        """
        :param url: URL to connect.
        :param jar_path: path to jar contains driver class name.
        :param jdbc_driver_name: Jdbc driver class name.
        """
        self.conn = SqlUtil.connection(url, jar_path, jdbc_driver_name)

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

    @staticmethod
    def connection(url: str,
                   jar_path: str,
                   jdbc_driver_name: str = 'org.apache.ignite.IgniteJdbcThinDriver'):
        """
        :param url: URL to connect.
        :param jar_path: path to jar contains driver class name.
        :param jdbc_driver_name: Jdbc driver class name.
        :return Connection.
        """
        return jaydebeapi.connect(jclassname=jdbc_driver_name,
                                  url=url,
                                  jars=jar_path)
