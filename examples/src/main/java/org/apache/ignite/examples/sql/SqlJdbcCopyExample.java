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
            print("Connected to server.");

            // Create database objects.
            try (Statement stmt = conn.createStatement()) {
                // Create reference City table based on REPLICATED template.
                stmt.executeUpdate("CREATE TABLE city (id LONG PRIMARY KEY, name VARCHAR) " +
                    "WITH \"template=replicated\"");

                // Create table based on PARTITIONED template with one backup.
                stmt.executeUpdate("CREATE TABLE person (id LONG, name VARCHAR, city_id LONG, " +
                    "PRIMARY KEY (id, city_id)) WITH \"backups=1, affinity_key=city_id\"");
            }

            print("Created database objects.");

            // Populate City via COPY command with records from cityBulkLoad.csv
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("COPY FROM \"" +
                    IgniteUtils.resolveIgnitePath("examples/src/main/resources/cityBulkLoad.csv") + "\" " +
                    "INTO City (id, name) FORMAT CSV");
            }

            // Populate Person via COPY command with records from personBulkLoad.csv
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("COPY FROM \"" +
                    IgniteUtils.resolveIgnitePath("examples/src/main/resources/personBulkLoad.csv") + "\" " +
                    "INTO Person (id, name, city_id) FORMAT CSV");
            }

            print("Populated data via COPY.");

            // Get data.
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs =
                    stmt.executeQuery("SELECT p.name, c.name FROM Person p INNER JOIN City c on c.id = p.city_id")) {
                    print("Query results:");

                    while (rs.next())
                        System.out.println(">>>    " + rs.getString(1) + ", " + rs.getString(2));
                }
            }

            // Drop database objects.
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DROP TABLE Person");
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
}