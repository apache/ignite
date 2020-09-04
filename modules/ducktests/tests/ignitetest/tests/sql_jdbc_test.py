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
import jaydebeapi

from random import randint
from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, LATEST_2_7, V_2_8_0, V_2_8_1, IgniteVersion


# pylint: disable=W0223
class SqlJdbcTest(IgniteTest):
    """
    Tests SQL.
    """
    NUM_NODES = 3

    @cluster(num_nodes=NUM_NODES)
    @parametrize(version=str(DEV_BRANCH))
    @parametrize(version=str(V_2_8_1))
    @parametrize(version=str(V_2_8_0))
    @parametrize(version=str(LATEST_2_7))
    def sql_test(self, version):
        """
        Test SQL.
        """
        self.stage("Starting nodes")

        ignite_version = IgniteVersion(version)

        config = IgniteConfiguration(version=IgniteVersion(version))

        ignites = IgniteService(self.test_context, config=config, num_nodes=self.NUM_NODES)

        ignites.start()

        randint(0, self.NUM_NODES - 1)

        node = ignites.nodes[randint(0, self.NUM_NODES - 1)]

        ignites.pids(node)

        if ignite_version == DEV_BRANCH:
            core_jar_path = '/opt/ignite-dev/modules/core/target/ignite-core-2.10.0-SNAPSHOT.jar'
        else:
            core_jar_path = str(ignite_core_jar(ignites, node))

        self.logger.info("Path to ignite-core.java: " + core_jar_path)

        _ip = node.account.externally_routable_ip

        url = "jdbc:ignite:thin://" + _ip

        self.stage("Get connection to " + url)

        conn = jaydebeapi.connect(jclassname='org.apache.ignite.IgniteJdbcThinDriver',
                                  url=url,
                                  jars=core_jar_path)

        curs = conn.cursor()

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

        curs.close()
        conn.close()


def insert(curs, size=100):
    """
    SQL insert.
    :param curs: Сursor obtained from the connection.
    """
    for i in range(size):
        curs.execute(
            "INSERT INTO  users (_key, name) VALUES(CAST({0} as BIGINT), 'User{0}')".format(i))

    curs.execute("select name from users")

    assert len(curs.fetchall()) == size


def update(curs, size=100):
    """
    SQL update.
    :param curs: Сursor obtained from the connection.
    """
    for i in range(size):
        curs.execute("UPDATE users SET name = 'newUser{0}' where id = {0}".format(i))

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
    for i in range(size):
        curs.execute("DELETE FROM users where id = {0}".format(i))

    curs.execute("select name from users")

    assert len(curs.fetchall()) == 0


def ignite_core_jar(service, node):
    """
    :param service: IgniteService.
    :param node: Node.
    :return: Path to ignite-core.jar.
    """
    pids = service.pids(node)

    cmd = "ls -al /proc/%s/fd | grep -Eo '/opt/ignite.*/ignite-core.*.jar'" % pids[0]

    return list(node.account.ssh_capture(cmd, allow_fail=True, callback=str))[0].rstrip()
