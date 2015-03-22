/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.examples.datagrid.store.jdbc;

import org.apache.ignite.*;
import org.apache.ignite.cache.store.jdbc.*;
import org.apache.ignite.examples.datagrid.store.model.*;
import org.h2.tools.*;

import javax.cache.*;
import java.io.*;
import java.sql.*;

/**
 * Example of {@link CacheJdbcPojoStore} implementation that uses JDBC
 * transaction with cache transactions and maps {@link Long} to {@link Person}.
 */
public class CacheJdbcPojoPersonStore extends CacheJdbcPojoStore<Long, Person> {
    /**
     * Constructor.
     *
     * @throws IgniteException If failed.
     */
    public CacheJdbcPojoPersonStore() throws IgniteException {
        try {
            // Try to connect to database server.
            dataSrc = org.h2.jdbcx.JdbcConnectionPool.create("jdbc:h2:tcp://localhost/mem:ExampleDb", "sa", "");

            resolveDialect();
        }
        catch (CacheException ignore) {
            // Construct example database in memory.
            dataSrc = org.h2.jdbcx.JdbcConnectionPool.create("jdbc:h2:mem:ExampleDb;DB_CLOSE_DELAY=-1", "sa", "");

            prepareDb();
        }
    }

    /** */
    private static final String DB_SCRIPT =
        "create table PERSON(id bigint not null, first_name varchar(50), last_name varchar(50), PRIMARY KEY(id));\n" +
        "insert into PERSON(id, first_name, last_name) values(1, 'Johannes', 'Kepler');\n" +
        "insert into PERSON(id, first_name, last_name) values(2, 'Galileo', 'Galilei');\n" +
        "insert into PERSON(id, first_name, last_name) values(3, 'Henry', 'More');\n" +
        "insert into PERSON(id, first_name, last_name) values(4, 'Polish', 'Brethren');\n" +
        "insert into PERSON(id, first_name, last_name) values(5, 'Robert', 'Boyle');\n" +
        "insert into PERSON(id, first_name, last_name) values(6, 'Isaac', 'Newton');";

    /**
     * Prepares database for example execution. This method will create a table called "PERSONS"
     * so it can be used by store implementation.
     *
     * @throws IgniteException If failed.
     */
    private void prepareDb() throws IgniteException {
        try {
            // Start H2 database TCP server in order to access sample in-memory database from other processes.
            Server.createTcpServer("-tcpDaemon").start();

            // Load sample data into database.
            RunScript.execute(dataSrc.getConnection(), new StringReader(DB_SCRIPT));
        }
        catch (SQLException e) {
            throw new IgniteException("Failed to initialize database", e);
        }
    }
}
