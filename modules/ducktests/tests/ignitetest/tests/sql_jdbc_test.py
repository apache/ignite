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
from ignitetest.services.ignite import IgniteService
from ignitetest.services.utils.ignite_configuration import IgniteConfiguration
from ignitetest.services.utils.sql_util import jdbc_connection
from ignitetest.utils import version_with_previous
from ignitetest.utils.ignite_test import IgniteTest
from ignitetest.utils.version import DEV_BRANCH, V_2_8_1, V_2_8_0, V_2_7_6, IgniteVersion


# pylint: disable=W0223
class SqlJdbcTest(IgniteTest):
    """
    SQL tests using the JDBC driver.
    """
    NUM_NODES = 3

    @cluster(num_nodes=NUM_NODES)
    @version_with_previous(str(DEV_BRANCH), str(V_2_8_1), str(V_2_8_0), str(V_2_7_6))
    def sql_test(self, ignite_version_1, ignite_version_2):
        """
        SQL test with previous versions jdbc driver.
        :param ignite_version_1: Version ignite service.
        :param ignite_version_2: Version JDBC driver.
        """
        self.stage("Starting nodes")

        config = IgniteConfiguration(version=IgniteVersion(ignite_version_1))

        service = IgniteService(self.test_context, config=config, num_nodes=self.NUM_NODES)
        service.start()

        with jdbc_connection(ignite_service=service, ver=IgniteVersion(ignite_version_2)) as conn:
            with conn.cursor() as curs:
                create_tables(curs)
                self.logger.info("Created tables.")

                insert(curs)
                self.logger.info("Inserted.")

                update(curs)
                self.logger.info("Updated.")

                alter_table_add(curs)
                self.logger.info("Alter table add.")

                join(curs)
                self.logger.info("Distributed join.")

                alter_table_drop(curs)
                self.logger.info("Alter table drop.")

                delete(curs)
                self.logger.info("Deleted.")

                drop_tables(curs)
                self.logger.info("Deleted tables.")


def create_tables(curs):
    """
    Create tables for test.
    :param curs: Сursor obtained from the connection.
    """
    curs.execute('CREATE TABLE users (id int, name varchar, PRIMARY KEY (id))')
    curs.execute('CREATE TABLE organization (id int, name varchar, PRIMARY KEY (id))')


def insert(curs, size=100):
    """
    Insert.
    :param curs: Сursor obtained from the connection.
    """
    users = [(i, 'User' + str(i)) for i in range(size)]

    curs.executemany("INSERT INTO  users (_key, name) VALUES(CAST( ? as BIGINT), ?)", users)

    curs.execute("select name from users")

    assert len(curs.fetchall()) == size


def update(curs, size=100):
    """
    Update.
    :param curs: Сursor obtained from the connection.
    """
    users = [('newUser' + str(i), i) for i in range(size)]

    curs.executemany("UPDATE users SET name = ? where id = ? ", users)

    curs.execute("select name from users")

    names = curs.fetchall()

    for name in names:
        assert str(name[0]).startswith("newUser")

    assert len(names) == size


def alter_table_add(curs):
    """
    Added column using alter table.
    :param curs: Сursor obtained from the connection.
    """
    curs.execute('ALTER TABLE users ADD COLUMN organization varchar')


def join(curs, size=100):
    """
    Distributed join.
    :param curs: Сursor obtained from the connection.
    """
    org1 = 'org1'
    org2 = 'org2'

    curs.execute("INSERT INTO  organization (_key, name) VALUES(CAST( 1 as BIGINT), '%s')" % org1)
    curs.execute("INSERT INTO  organization (_key, name) VALUES(CAST( 2 as BIGINT), '%s')" % org2)

    curs.execute('CREATE INDEX us_name_idx ON users (name)')
    curs.execute('CREATE INDEX org_name_idx ON organization (name)')
    curs.execute('CREATE INDEX us_org_idx ON users (organization)')

    half = size / 2

    curs.execute("UPDATE users SET organization = '%s' where id < %i " % (org1, half))

    curs.execute("SELECT us.name, org.name FROM organization as org JOIN users as us ON us.organization = org.name")

    assert len(curs.fetchall()) == half

    curs.execute(
        "SELECT us.name, org.name FROM organization as org LEFT JOIN users as us ON us.organization = org.name")

    assert len(curs.fetchall()) == (half + 1)

    curs.execute(
        "SELECT us.name, org.name FROM organization as org RIGHT JOIN users as us ON us.organization = org.name")

    assert len(curs.fetchall()) == size

    curs.execute('DROP INDEX us_name_idx')
    curs.execute('DROP INDEX us_org_idx')
    curs.execute('DROP INDEX org_name_idx')


def alter_table_drop(curs):
    """
    Dropped column using alter table.
    :param curs: Сursor obtained from the connection.
    """
    curs.execute('ALTER TABLE users DROP organization')


def delete(curs, size=100):
    """
    Delete.
    :param curs: Сursor obtained from the connection.
    """
    ids = [[x] for x in range(size)]

    curs.executemany("DELETE FROM users where id = CAST( ? as BIGINT)", ids)

    curs.execute("select name from users")

    names = curs.fetchall()

    assert len(names) == 0


def drop_tables(curs):
    """
    Dropped tables.
    :param curs: Сursor obtained from the connection.
    """
    curs.execute('DROP TABLE users')
    curs.execute('DROP TABLE organization')
