package org.apache.ignite.snippets;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;

public class JDBCClientDriver {

    @Test
    void registerDriver() throws ClassNotFoundException, SQLException {
        //tag::register[]
        // Registering the JDBC driver.
        Class.forName("org.apache.ignite.IgniteJdbcDriver");

        // Opening JDBC connection (cache name is not specified, which means that we use default cache).
        Connection conn = DriverManager.getConnection("jdbc:ignite:cfg://config/ignite-jdbc.xml");
        
        //end::register[]
        conn.close();
    }

    @Test
    void streaming() throws ClassNotFoundException, SQLException {
        // Register JDBC driver.
        Class.forName("org.apache.ignite.IgniteJdbcDriver");

        // Opening connection in the streaming mode.
        Connection conn = DriverManager
                .getConnection("jdbc:ignite:cfg://cache=myCache:streaming=true@file://config/ignite-jdbc.xml");

        conn.close();
    }
    
    @Test
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
