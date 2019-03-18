/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This sample application shows how to cache prepared statements.
 */
public class CachedPreparedStatements {

    private Connection conn;
    private Statement stat;
    private final Map<String, PreparedStatement> prepared =
        Collections.synchronizedMap(
                new HashMap<String, PreparedStatement>());

    /**
     * This method is called when executing this sample application from the
     * command line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        new CachedPreparedStatements().run();
    }

    private void run() throws Exception {
        Class.forName("org.h2.Driver");
        conn = DriverManager.getConnection(
                "jdbc:h2:mem:", "sa", "");
        stat = conn.createStatement();
        stat.execute(
                "create table test(id int primary key, name varchar)");
        PreparedStatement prep = prepare(
                "insert into test values(?, ?)");
        prep.setInt(1, 1);
        prep.setString(2, "Hello");
        prep.execute();
        conn.close();
    }

    private PreparedStatement prepare(String sql)
            throws SQLException {
        PreparedStatement prep = prepared.get(sql);
        if (prep == null) {
            prep = conn.prepareStatement(sql);
            prepared.put(sql, prep);
        }
        return prep;
    }

}
