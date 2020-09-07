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
This module contains SQL tests using the JDBC driver.
"""
from ducktape.mark.resource import cluster
from ignitetest.utils import ignite_versions
from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.sql_util import connection
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, V_2_8_1, V_2_8_0, V_2_7_6, IgniteVersion


# pylint: disable=W0223
class SqlJdbcTest(IgniteTest):
    """
    SQL tests using the JDBC driver.
    """
    NUM_NODES = 3

    @cluster(num_nodes=NUM_NODES)
    @ignite_versions(str(DEV_BRANCH), str(V_2_8_1), str(V_2_8_0), str(V_2_7_6))
    def sql_test(self, ignite_version):
        """
        Test SQL.
        """
        self.stage("Starting nodes")

        config = IgniteConfiguration(version=IgniteVersion(ignite_version))

        service = IgniteService(self.test_context, config=config, num_nodes=self.NUM_NODES)
        service.start()

        with connection(service) as conn:
            with conn.cursor() as curs:
                curs.execute('CREATE TABLE users (id int, name varchar, PRIMARY KEY (id))')
                self.logger.info("Created a table of users")

                curs.execute('CREATE INDEX name_idx ON users (name)')
                self.logger.info("Created a index")

                insert(curs)
                update(curs)
                delete(curs)

                curs.execute('DROP INDEX name_idx')
                self.logger.info("Droped a index")

                curs.execute('DROP TABLE users')
                self.logger.info("Deleted the users table")


def insert(curs, size=100):
    """
    SQL insert.
    :param curs: Сursor obtained from the connection.
    """
    users = [(i, 'User' + str(i)) for i in range(size)]

    curs.executemany("INSERT INTO  users (_key, name) VALUES(CAST( ? as BIGINT), ?)", users)

    curs.execute("select name from users")

    assert len(curs.fetchall()) == size


def update(curs, size=100):
    """
    SQL update.
    :param curs: Сursor obtained from the connection.
    """
    users = [('newUser' + str(i), i) for i in range(size)]

    curs.executemany("UPDATE users SET name = ? where id = ? ", users)

    curs.execute("select name from users")

    names = curs.fetchall()

    for name in names:
        assert str(name[0]).startswith("newUser")

    assert len(names) == size


def delete(curs, size=100):
    """
    SQL delete.
    :param curs: Сursor obtained from the connection.
    """
    ids = [[x] for x in range(size)]

    curs.executemany("DELETE FROM users where id = CAST( ? as BIGINT)", ids)

    curs.execute("select name from users")

    names = curs.fetchall()

    assert len(names) == 0