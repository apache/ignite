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

package org.apache.ignite.examples.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * This example demonstrates usage of COPY command via Ignite thin JDBC driver.
 * <p>
 * Ignite nodes must be started in separate process using {@link ExampleNodeStartup} before running this example.
 */
public class SqlJdbcCopyExample {
    /**
     * Executes JDBC COPY example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        print("JDBC COPY example started.");

        // Open JDBC connection
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DROP TABLE IF EXISTS City");
            }


            print("Connected to server.");

            executeCommand(conn,
                "CREATE TABLE City (" +
                "    ID INT(11), " +
                "    Name CHAR(35), " +
                "    CountryCode CHAR(3), " +
                "    District CHAR(20), " +
                "    Population INT(11), " +
                "    PRIMARY KEY (ID, CountryCode) " +
                ") WITH \"template=partitioned, backups=1, affinityKey=CountryCode, CACHE_NAME=City\""
            );

            print("Created database objects.");

            executeCommand(conn, "COPY FROM \"" +
                IgniteUtils.resolveIgnitePath("examples/sql/city.csv") + "\" " +
                "INTO City (ID, Name, CountryCode, District, Population) FORMAT CSV");

            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM City")) {
                    rs.next();

                    print("Populated City table: " + rs.getLong(1) + " entries");
                }
            }

            // Drop database objects.
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DROP TABLE City");
            }

            print("Dropped database objects.");
        }

        print("JDBC COPY example finished.");
    }

    /**
     * Prints message.
     *
     * @param msg Message to print before all objects are printed.
     */
    private static void print(String msg) {
        System.out.println();
        System.out.println(">>> " + msg);
    }

    /**
     * Execute SQL command.
     *
     * @param conn Connection.
     * @param sql SQL statement.
     * @throws Exception If failed.
     */
    private static void executeCommand(Connection conn, String sql) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }
}