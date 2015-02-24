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

import org.apache.ignite.*;
import org.apache.ignite.examples.datagrid.store.model.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.h2.tools.*;

import java.sql.*;
import java.util.*;

/**
 * This examples demonstrates loading data into cache from underlying JDBC store.
 */
public class CacheJdbcPojoStoreExample {
    /** DB connection URL. */
    private static final String CONN_URL = "jdbc:h2:mem:ExampleDb;DB_CLOSE_DELAY=-1";

    /** Cache name. */
    private static final String CACHE_NAME = "partitioned";

    /** Number of generated organizations. */
    private static final int ORGANIZATION_CNT = 5;

    /** Number of generated persons. */
    private static final int PERSON_CNT = 100;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        System.out.println("Populate database with sample data.");

        createDb();

        // Start H2 database TCP server in order to access sample in-memory database from other processes.
        // This is H2 specific only.
        Server srv = Server.createTcpServer().start();

        // Start node and load cache from database.
        try (Ignite ignite = Ignition.start("examples/config/store/example-jdbc-pojo-store.xml")) {
            IgniteCache<Object, Object> cache = ignite.jcache(CACHE_NAME);

            System.out.println("Load whole DB into cache.");

            cache.loadCache(null);

            System.out.println("Print loaded content.");

            System.out.println("Organizations:");
            for (int i = 0; i < ORGANIZATION_CNT; i++) {
                OrganizationKey orgKey = new OrganizationKey(i);

                System.out.println("    " + cache.get(orgKey));
            }

            System.out.println("Persons:");
            for (int i = 0; i < PERSON_CNT; i++) {
                PersonKey prnKey = new PersonKey(i);

                System.out.println("    " + cache.get(prnKey));
            }

            System.out.println("Clear cache for next demo.");

            cache.clear();

            System.out.println("Cache size = " + cache.size());

            System.out.println("Load cache by custom SQL.");

            // JDBC cache store accept pairs of "full key class name -> SQL statement"
            cache.loadCache(null,
                "org.apache.ignite.examples.datagrid.store.model.OrganizationKey",
                "SELECT * FROM Organization WHERE id = 2",
                "org.apache.ignite.examples.datagrid.store.model.PersonKey",
                "SELECT * FROM Person WHERE id = 5");

            System.out.println("Check custom SQL.");
            System.out.println("    Organization: " + cache.get(new OrganizationKey(2)));
            System.out.println("    Person: " + cache.get(new PersonKey(5)));
        }

        // Stop H2 TCP server. H2 specific only.
        srv.stop();

        System.exit(0);
    }

    /**
     * Create example DB and populate it with sample data.
     *
     * @throws Exception If failed to create databse and populate it with sample data.
     */
    private static void createDb() throws Exception {
        Class.forName("org.h2.Driver");

        Connection conn = DriverManager.getConnection(CONN_URL, "sa", "");

        Statement stmt = conn.createStatement();

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS Organization" +
            "(id integer not null, name varchar(50), city varchar(50), PRIMARY KEY(id))");

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS Person" +
            "(id integer not null, org_id integer, name varchar(50), PRIMARY KEY(id))");

        U.closeQuiet(stmt);

        conn.commit();

        PreparedStatement orgStmt = conn.prepareStatement("INSERT INTO Organization(id, name, city) VALUES (?, ?, ?)");

        for (int i = 0; i < ORGANIZATION_CNT; i++) {
            orgStmt.setInt(1, i);
            orgStmt.setString(2, "org-name-" + i);
            orgStmt.setString(3, "city-" + i);

            orgStmt.addBatch();
        }

        orgStmt.executeBatch();

        U.closeQuiet(orgStmt);

        conn.commit();

        PreparedStatement prnStmt = conn.prepareStatement("INSERT INTO Person(id, org_id, name) VALUES (?, ?, ?)");

        Random rnd = new Random();

        for (int i = 0; i < PERSON_CNT; i++) {
            prnStmt.setInt(1, i);
            prnStmt.setInt(2, rnd.nextInt(ORGANIZATION_CNT));
            prnStmt.setString(3, "person-name-" + i);

            prnStmt.addBatch();
        }

        prnStmt.executeBatch();

        U.closeQuiet(prnStmt);

        conn.commit();

        U.closeQuiet(conn);
    }
}
