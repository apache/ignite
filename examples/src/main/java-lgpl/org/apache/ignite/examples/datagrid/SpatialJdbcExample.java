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
import java.sql.Statement;
import java.util.Random;
import org.apache.ignite.examples.ExampleNodeStartup;

/**
 * This examples shows the usage of geospatial queries and indexes in Apache Ignite.
 * For more information please refer to the following technical documentation:
 * http://apacheignite.readme.io/docs/geospatial-queries
 * <p>
 * Remote nodes should be started using {@link ExampleNodeStartup} which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public class SpatialJdbcExample {
    /** Cache name. */
    private static final String CACHE_NAME = SpatialQueryExample.class.getSimpleName();

    /**
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) throws Exception {
        print("JDBC example started.");

        // Open JDBC connection
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/"))  {
            print("Connected to server.");
            // Create database objects.
            try (Statement stmt = conn.createStatement()) {
                // Create reference City table based on REPLICATED template.
                stmt.executeUpdate("CREATE TABLE Point (id LONG PRIMARY KEY, geom GEOMETRY) " +
                    "WITH \"template=replicated\"");

                // Create an index.
                stmt.executeUpdate("CREATE SPATIAL INDEX on Point (geom)");
            }


            Random rnd = new Random();
            try(PreparedStatement stmt = conn.prepareStatement("INSERT INTO Point (id, geom) VALUES (?, ?)")) {

                // Adding geometry points into the table.
                for (long i = 0; i < 1000; i++) {
                    int x = rnd.nextInt(10000);
                    int y = rnd.nextInt(10000);

                    stmt.setLong(1, i);
                    stmt.setObject(2, "POINT(" + x + " " + y + ")");
                    stmt.addBatch();

                }

                stmt.executeBatch();
            }
            print("Populated data.");

            String poly = "POLYGON((0 0, 0 " + rnd.nextInt(10000) + ", " +
                rnd.nextInt(10000) + " " + rnd.nextInt(10000) + ", " +
                rnd.nextInt(10000) + " 0, 0 0))";

            // Query points that are inside polygon and print it's geometry
            try(PreparedStatement stmt = conn.prepareStatement("SELECT id, geom FROM Point where geom && ?")){
                print("Fetch points in " + poly);
                stmt.setObject(1, poly);

                try(ResultSet rs = stmt.executeQuery()){
                    print("Query results:");
                    while(rs.next())
                        print("id:" + rs.getInt(1) + " " + " geom: " + rs.getObject(2));
                }
            }

            // Get total number of points inside polygon
            try(PreparedStatement stmt = conn.prepareStatement("SELECT count(*) FROM Point where geom && ?")){
                print("Total points in " + poly + " :");
                stmt.setObject(1, poly);

                try(ResultSet rs = stmt.executeQuery()){
                    while(rs.next())
                        print(Integer.valueOf(rs.getInt(1)).toString());
                }
            }


            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DROP TABLE Point");
            }
            print("Dropped Point table");
        }
        print("Spatial JDBC example finished.");
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