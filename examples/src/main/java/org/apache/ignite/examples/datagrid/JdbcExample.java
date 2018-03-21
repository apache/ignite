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

package org.apache.ignite.examples.datagrid;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.examples.ExampleNodeStartup;

/**
 * JDBC example. This example demonstrates Ignite JDBC interface.
 * <p>
 * Ignite nodes should be started using {@link ExampleNodeStartup} which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public class JdbcExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        // Open JDBC connection
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/")) {
            // Example for use DML queries (create tables).
            createTables(conn);

            // Example for use DDL query (create index).
            createIndex(conn);

            // Example for use DML queries (fill table).
            fillCityTable(conn);

            // Example for use DML queries (fill table).
            fillPersonTableWithBatch(conn);

            // Example for use SELECT query (select and print results).
            selectJoin(conn);
        }
    }

    /**
     * Example for DDL queries. Create tables.
     *
     * @param conn JDBC connection.
     * @throws SQLException On error.
     */
    private static void createTables(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // Drop tables if exists.
            stmt.execute("DROP TABLE IF EXISTS person");
            stmt.execute("DROP TABLE IF EXISTS city");

            // Create person table. PARTITIONED template is used.
            // Available PARTITIONED, REPLICATED & custom defined at the ignite configuration templates.
            stmt.execute("CREATE TABLE person " +
                "(id int, " +
                "name varchar, " +
                "age int, " +
                "company varchar, " +
                "city int, " +
                "primary key (id, name, city)) " +
                "WITH \"template=PARTITIONED,atomicity=ATOMIC,affinitykey=city\"");

            stmt.execute("CREATE TABLE city " +
                "(id int, " +
                "name varchar, " +
                "population int, " +
                "primary key (id))");
        }
    }

    /**
     * Example for DML queries. Create index.
     *
     * @param conn JDBC connection.
     * @throws SQLException On error.
     */
    private static void createIndex(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // Create index.
            stmt.execute("CREATE INDEX IF NOT EXISTS idx on person (city asc, name asc)");
        }
    }

    /**
     * Example for DML queries. Fill table.
     *
     * @param conn JDBC connection.
     * @throws SQLException On error.
     */
    private static void fillCityTable(Connection conn) throws SQLException {
        // Adds 3 rows into 'city' table.
        try (PreparedStatement stmt =
                 conn.prepareStatement("INSERT INTO city (id, name, population) values (?, ?, ?), (?, ?, ?), (?, ?, ?)")) {
            int argCnt = 1;

            // Sets the query parameters.
            stmt.setInt(argCnt++, 0);
            stmt.setString(argCnt++, "St. Petersburg");
            stmt.setInt(argCnt++, 6000000);
            stmt.setInt(argCnt++, 1);
            stmt.setString(argCnt++, "Boston");
            stmt.setInt(argCnt++, 2000000);
            stmt.setInt(argCnt++, 2);
            stmt.setString(argCnt++, "London");
            stmt.setInt(argCnt++, 8000000);

            // Execute the query.
            int updCnt = stmt.executeUpdate();

            System.out.println("city table: " + updCnt + " rows are updated");
        }
    }

    /**
     * Example for DML queries. Fill table.
     *
     * @param conn JDBC connection.
     * @throws SQLException On error.
     */
    private static void fillPersonTableWithBatch(Connection conn) throws SQLException {
        final List<String> COMPANIES = Arrays.asList("Ignite", "Oracle", "Google");

        final List<String> CITIES = Arrays.asList("St. Petersburg", "Boston", "Berkeley", "London");

        int updCnt = 0;

        try (PreparedStatement stmt = conn.prepareStatement(
            "INSERT INTO person (id, name, age, company, city) values (?, ?, ?, ?, ?)")) {

            int id = 0;

            // Adds 100 rows into 'person' table.
            for (int i = 0; i < 100; ++i) {
                int argCnt = 1;

                stmt.setInt(argCnt++, id++); // id
                stmt.setString(argCnt++, "Person" + i); // name
                stmt.setInt(argCnt++, 20 + (i % 10)); // age
                stmt.setString(argCnt++, COMPANIES.get(i % COMPANIES.size())); // company
                stmt.setInt(argCnt++, i % CITIES.size()); // city

                updCnt += stmt.executeUpdate();
            }
        } finally {
            System.out.println("person table: " + updCnt + " rows are updated");
        }
    }

    /**
     * Example for SELECT query.
     *
     * @param conn JDBC connection.
     * @throws SQLException On error.
     */
    private static void selectJoin(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT * from Person p inner join City c on p.city = c.id");

            System.out.println("Results:");
            printResultSet(rs);
        }
    }

    /**
     * Print ResultSet as CSV.
     *
     * @param rs Result set.
     * @throws SQLException On error.
     */
    private static void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();

        // Gets the number of columns in the results.
        int cols = meta.getColumnCount();

        System.out.print(meta.getColumnName(1));

        for (int i = 1; i < cols; ++i)
            System.out.print(", " + meta.getColumnName(i + 1));

        System.out.println("");

        while (rs.next()) {
            System.out.print(String.valueOf(rs.getObject(1)));

            for (int i = 1; i < cols; ++i)
                System.out.print(", " + String.valueOf(rs.getObject(i + 1)));

            System.out.println("");
        }
    }
}

