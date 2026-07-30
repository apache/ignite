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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteJdbcThinDataSource;
import org.apache.ignite.Ignition;

public class JDBCThinDriver {

    Connection getConnection() throws ClassNotFoundException, SQLException {

        // tag::get-connection[]
        // Register JDBC driver.
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

        // Open the JDBC connection.
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");

        // end::get-connection[]
        return conn;
    }

    void multipleEndpoints() throws ClassNotFoundException, SQLException {
        // tag::multiple-endpoints[]

        // Register JDBC Driver.
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

        // Open the JDBC connection passing several connection endpoints.
        Connection conn = DriverManager
                .getConnection("jdbc:ignite:thin://192.168.0.50:101,192.188.5.40:101,192.168.10.230:101");
        // end::multiple-endpoints[]

    }

    public Connection connectionFromDatasource() throws SQLException {
        // tag::connection-from-data-source[]
        // Or open connection via DataSource.
        IgniteJdbcThinDataSource ids = new IgniteJdbcThinDataSource();
        ids.setUrl("jdbc:ignite:thin://127.0.0.1");
        ids.setDistributedJoins(true);

        Connection conn = ids.getConnection();
        // end::connection-from-data-source[]

        return conn;
    }

    void select() throws ClassNotFoundException, SQLException {

        Connection conn = getConnection();

        // tag::select[]
        // Query people with specific age using prepared statement.
        PreparedStatement stmt = conn.prepareStatement("select name, age from Person where age = ?");

        stmt.setInt(1, 30);

        ResultSet rs = stmt.executeQuery();

        while (rs.next()) {
            String name = rs.getString("name");
            int age = rs.getInt("age");
            // ...
        }
        // end::select[]
        conn.close();
    }

    void insert() throws ClassNotFoundException, SQLException {

        Connection conn = getConnection();
        // tag::insert[]
        // Insert a Person with a Long key.
        PreparedStatement stmt = conn
                .prepareStatement("INSERT INTO Person(_key, name, age) VALUES(CAST(? as BIGINT), ?, ?)");

        stmt.setInt(1, 1);
        stmt.setString(2, "John Smith");
        stmt.setInt(3, 25);

        stmt.execute();
        // end::insert[]
        conn.close();
    }

    void merge() throws ClassNotFoundException, SQLException {

        Connection conn = getConnection();
        // tag::merge[]
        // Merge a Person with a Long key.
        PreparedStatement stmt = conn
                .prepareStatement("MERGE INTO Person(_key, name, age) VALUES(CAST(? as BIGINT), ?, ?)");

        stmt.setInt(1, 1);
        stmt.setString(2, "John Smith");
        stmt.setInt(3, 25);

        stmt.executeUpdate();
        // end::merge[]
        conn.close();
    }

    void partitionAwareness() throws ClassNotFoundException, SQLException {

        // tag::partition-awareness[]
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

        Connection conn = DriverManager
                .getConnection("jdbc:ignite:thin://192.168.0.50,192.188.5.40,192.168.10.230?partitionAwareness=true");
        // end::partition-awareness[]

        conn.close();
    }

    void blobAndClob() throws ClassNotFoundException, SQLException {

        Connection conn = getConnection();
        // tag::blob-clob[]
        // CLOB values are stored in VARCHAR columns, and BLOB values are stored in BINARY columns.
        conn.createStatement().executeUpdate(
                "CREATE TABLE IF NOT EXISTS DocumentStore (id INT PRIMARY KEY, payload BINARY, body VARCHAR)");

        byte[] payload = "binary payload".getBytes(StandardCharsets.UTF_8);

        PreparedStatement insert = conn.prepareStatement(
                "INSERT INTO DocumentStore(id, payload, body) VALUES (?, ?, ?)");

        Blob blob = conn.createBlob();
        blob.setBytes(1, payload);

        Clob clob = conn.createClob();
        clob.setString(1, "large text body");

        insert.setInt(1, 1);
        insert.setBlob(2, blob);
        insert.setClob(3, clob);
        insert.executeUpdate();

        PreparedStatement streamInsert = conn.prepareStatement(
                "INSERT INTO DocumentStore(id, payload, body) VALUES (?, ?, ?)");

        streamInsert.setInt(1, 2);
        streamInsert.setBlob(2, new ByteArrayInputStream(payload), payload.length);
        streamInsert.setString(3, "large text body");
        streamInsert.executeUpdate();

        PreparedStatement select = conn.prepareStatement("SELECT payload, body FROM DocumentStore WHERE id = ?");

        select.setInt(1, 1);

        ResultSet rs = select.executeQuery();

        if (rs.next()) {
            Blob selectedBlob = rs.getBlob("payload");
            byte[] selectedPayload = selectedBlob.getBytes(1, (int)selectedBlob.length());

            Clob selectedClob = rs.getClob("body");
            String selectedBody = selectedClob.getSubString(1, (int)selectedClob.length());

            InputStream selectedPayloadStream = rs.getBinaryStream("payload");

            assert selectedPayload.length == payload.length;
            assert selectedBody.equals("large text body");
            assert selectedPayloadStream != null;
        }
        // end::blob-clob[]
        conn.close();
    }

    void handleException() throws ClassNotFoundException, SQLException {

        Connection conn = getConnection();
        // tag::handle-exception[]
        PreparedStatement ps;

        try {
            ps = conn.prepareStatement("INSERT INTO Person(id, name, age) values (1, 'John', 'unparseableString')");
        } catch (SQLException e) {
            switch (e.getSQLState()) {
            case "0700B":
                System.out.println("Conversion failure");
                break;

            case "42000":
                System.out.println("Parsing error");
                break;

            default:
                System.out.println("Unprocessed error: " + e.getSQLState());
                break;
            }
        }
        // end::handle-exception[]
    }

    void ssl() throws ClassNotFoundException, SQLException {

        //tag::ssl[]
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

        String keyStore = "keystore/node.jks";
        String keyStorePassword = "123456";

        String trustStore = "keystore/trust.jks";
        String trustStorePassword = "123456";

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1?sslMode=require"
                + "&sslClientCertificateKeyStoreUrl=" + keyStore + "&sslClientCertificateKeyStorePassword="
                + keyStorePassword + "&sslTrustCertificateKeyStoreUrl=" + trustStore
                + "&sslTrustCertificateKeyStorePassword=" + trustStorePassword)) {

            ResultSet rs = conn.createStatement().executeQuery("select 10");
            rs.next();
            System.out.println(rs.getInt(1));
        } catch (Exception e) {
            e.printStackTrace();
        }

        //end::ssl[]

    }

    void errorCodes() throws ClassNotFoundException, SQLException {
        //tag::error-codes[]
        // Register JDBC driver.
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

        // Open JDBC connection.
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1");

        PreparedStatement ps;

        try {
            ps = conn.prepareStatement("INSERT INTO Person(id, name, age) values (1," + "'John', 'unparseableString')");
        } catch (SQLException e) {
            switch (e.getSQLState()) {
            case "0700B":
                System.out.println("Conversion failure");
                break;

            case "42000":
                System.out.println("Parsing error");
                break;

            default:
                System.out.println("Unprocessed error: " + e.getSQLState());
                break;
            }
        }

        //end::error-codes[]
    }

    public static void main(String[] args) throws Exception {
        Ignite ignite = Ignition.start();
        try {
            JDBCThinDriver j = new JDBCThinDriver();

            j.blobAndClob();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            ignite.close();
        }
    }
}
