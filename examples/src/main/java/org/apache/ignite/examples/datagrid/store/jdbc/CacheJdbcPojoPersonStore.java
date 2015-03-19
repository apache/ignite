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
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.h2.tools.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.integration.*;
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

    /**
     * Prepares database for example execution. This method will create a table called "PERSONS"
     * so it can be used by store implementation.
     *
     * @throws IgniteException If failed.
     */
    private void prepareDb() throws IgniteException {
        File script = U.resolveIgnitePath("examples/config/store/example-database.script");

        if (script == null)
            throw new IgniteException("Failed to find example database script: " +
                "examples/config/store/example-database.script");

        try {
            // Start H2 database TCP server in order to access sample in-memory database from other processes.
            Server.createTcpServer("-tcpDaemon").start();

            // Load sample data into database.
            RunScript.execute(dataSrc.getConnection(), new FileReader(script));
        }
        catch (SQLException e) {
            throw new IgniteException("Failed to initialize database", e);
        }
        catch (FileNotFoundException e) {
            throw new IgniteException("Failed to find example database script: " + script.getPath(), e);
        }
    }
}
