package org.apache.ignite.snippets;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.ignite.IgniteJdbcThinDataSource;

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
        //        Ignite ignite = Util.startNode();
        try {
            JDBCThinDriver j = new JDBCThinDriver();

            //            j.getConnection();
            // j.multipleEndpoints();
            // j.connectionFromDatasource();
            // j.partitionAwareness();

            j.ssl();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            // ignite.close();
        }
    }
}
