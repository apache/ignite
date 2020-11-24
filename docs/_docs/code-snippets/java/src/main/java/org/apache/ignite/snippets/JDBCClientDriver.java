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
package org.apache.ignite.snippets;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;

public class JDBCClientDriver {

    void registerDriver() throws ClassNotFoundException, SQLException {
        //tag::register[]
        // Registering the JDBC driver.
        Class.forName("org.apache.ignite.IgniteJdbcDriver");

        // Opening JDBC connection (cache name is not specified, which means that we use default cache).
        Connection conn = DriverManager.getConnection("jdbc:ignite:cfg://config/ignite-jdbc.xml");
        
        //end::register[]
        conn.close();
    }

    void streaming() throws ClassNotFoundException, SQLException {
        // Register JDBC driver.
        Class.forName("org.apache.ignite.IgniteJdbcDriver");

        // Opening connection in the streaming mode.
        Connection conn = DriverManager
                .getConnection("jdbc:ignite:cfg://cache=myCache:streaming=true@file://config/ignite-jdbc.xml");

        conn.close();
    }
    
    void timeBasedFlushing() throws ClassNotFoundException, SQLException {
        //tag::time-based-flushing[]
     // Register JDBC driver.
        Class.forName("org.apache.ignite.IgniteJdbcDriver");

        // Opening a connection in the streaming mode and time based flushing set.
        Connection conn = DriverManager.getConnection("jdbc:ignite:cfg://streaming=true:streamingFlushFrequency=1000@file:///etc/config/ignite-jdbc.xml");

        PreparedStatement stmt = conn.prepareStatement(
          "INSERT INTO Person(_key, name, age) VALUES(CAST(? as BIGINT), ?, ?)");

        // Adding the data.
        for (int i = 1; i < 100000; i++) {
              // Inserting a Person object with a Long key.
              stmt.setInt(1, i);
              stmt.setString(2, "John Smith");
              stmt.setInt(3, 25);

              stmt.execute();
        }

        conn.close();

        // Beyond this point, all data is guaranteed to be flushed into the cache.

        //end::time-based-flushing[]
    }
    
}
