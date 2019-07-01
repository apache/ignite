/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * This example shows how to secure passwords
 * (both database passwords, and account passwords).
 */
public class SecurePassword {

    /**
     * This method is called when executing this sample application from the
     * command line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {

        Class.forName("org.h2.Driver");
        String url = "jdbc:h2:data/simple";
        String user = "sam";
        char[] password = {'t', 'i', 'a', 'E', 'T', 'r', 'p'};

        // This is the normal, but 'unsafe' way to connect:
        // the password may reside in the main memory for an undefined time,
        // or even written to disk (swap file):
        // Connection conn =
        //     DriverManager.getConnection(url, user, new String(password));

        // This is the most safe way to connect: the password is overwritten
        // after use
        Properties prop = new Properties();
        prop.setProperty("user", user);
        prop.put("password", password);
        Connection conn = DriverManager.getConnection(url, prop);

        // For security reasons, account passwords should not be stored directly
        // in a database. Instead, only the hash should be stored. Also,
        // PreparedStatements must be used to avoid SQL injection:
        Statement stat = conn.createStatement();
        stat.execute(
                "drop table account if exists");
        stat.execute(
                "create table account(" +
                "name varchar primary key, " +
                "salt binary default secure_rand(16), " +
                "hash binary)");
        PreparedStatement prep;
        prep = conn.prepareStatement("insert into account(name) values(?)");
        prep.setString(1, "Joe");
        prep.execute();
        prep.close();

        prep = conn.prepareStatement(
                "update account set " +
                "hash=hash('SHA256', stringtoutf8(salt||?), 10) " +
                "where name=?");
        prep.setString(1, "secret");
        prep.setString(2, "Joe");
        prep.execute();
        prep.close();

        prep = conn.prepareStatement(
                "select * from account " +
                "where name=? " +
                "and hash=hash('SHA256', stringtoutf8(salt||?), 10)");
        prep.setString(1, "Joe");
        prep.setString(2, "secret");
        ResultSet rs = prep.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getString("name"));
        }
        rs.close();
        prep.close();
        stat.close();
        conn.close();
    }

}
