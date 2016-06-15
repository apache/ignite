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

package org.apache.ignite.examples.util;

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import org.apache.ignite.IgniteException;
import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.tools.RunScript;
import org.h2.tools.Server;

/**
 * Start H2 database TCP server in order to access sample in-memory database from other processes.
 */
public class DbH2ServerStartup {
    /** Create table script. */
    private static final String CREATE_PERSON_TABLE =
        "create table if not exists PERSON(id bigint not null, first_name varchar(50), last_name varchar(50), PRIMARY KEY(id));";

    /** Sample data script. */
    private static final String POPULATE_PERSON_TABLE =
        "delete from PERSON;\n" +
        "insert into PERSON(id, first_name, last_name) values(1, 'Johannes', 'Kepler');\n" +
        "insert into PERSON(id, first_name, last_name) values(2, 'Galileo', 'Galilei');\n" +
        "insert into PERSON(id, first_name, last_name) values(3, 'Henry', 'More');\n" +
        "insert into PERSON(id, first_name, last_name) values(4, 'Polish', 'Brethren');\n" +
        "insert into PERSON(id, first_name, last_name) values(5, 'Robert', 'Boyle');\n" +
        "insert into PERSON(id, first_name, last_name) values(6, 'Wilhelm', 'Leibniz');";

    /**
     * Populate sample database.
     *
     * @throws SQLException if
     */
    public static void populateDatabase() throws SQLException {
        // Try to connect to database TCP server.
        JdbcConnectionPool dataSrc = JdbcConnectionPool.create("jdbc:h2:tcp://localhost/mem:ExampleDb", "sa", "");

        // Create Person table in database.
        RunScript.execute(dataSrc.getConnection(), new StringReader(CREATE_PERSON_TABLE));

        // Populates Person table with sample data in database.
        RunScript.execute(dataSrc.getConnection(), new StringReader(POPULATE_PERSON_TABLE));
    }

    /**
     * Start H2 database TCP server.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If start H2 database TCP server failed.
     */
    public static void main(String[] args) throws IgniteException {
        try {
            // Start H2 database TCP server in order to access sample in-memory database from other processes.
            Server.createTcpServer("-tcpDaemon").start();

            populateDatabase();

            // Try to connect to database TCP server.
            JdbcConnectionPool dataSrc = JdbcConnectionPool.create("jdbc:h2:tcp://localhost/mem:ExampleDb", "sa", "");

            // Create Person table in database.
            RunScript.execute(dataSrc.getConnection(), new StringReader(CREATE_PERSON_TABLE));

            // Populates Person table with sample data in database.
            RunScript.execute(dataSrc.getConnection(), new StringReader(POPULATE_PERSON_TABLE));
        }
        catch (SQLException e) {
            throw new IgniteException("Failed to start database TCP server", e);
        }

        try {
            do {
                System.out.println("Type 'q' and press 'Enter' to stop H2 TCP server...");
            }
            while ('q' != System.in.read());
        }
        catch (IOException ignored) {
            // No-op.
        }
    }
}
