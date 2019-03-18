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
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.tools.TriggerAdapter;

/**
 * This sample application shows how to use triggers to create updatable views.
 */
public class UpdatableView extends TriggerAdapter {

    private PreparedStatement prepDelete, prepInsert;

    /**
     * This method is called when executing this sample application from the
     * command line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:mem:");
        Statement stat;
        stat = conn.createStatement();

        // create the table and the view
        stat.execute("create table test(id int primary key, name varchar)");
        stat.execute("create view test_view as select * from test");

        // create the trigger that is called whenever
        // the data in the view is modified
        stat.execute("create trigger t_test_view instead of " +
                "insert, update, delete on test_view for each row " +
                "call \"" + UpdatableView.class.getName() + "\"");

        // test a few operations
        stat.execute("insert into test_view values(1, 'Hello'), (2, 'World')");
        stat.execute("update test_view set name = 'Hallo' where id = 1");
        stat.execute("delete from test_view where id = 2");

        // print the contents of the table and the view
        System.out.println("table test:");
        ResultSet rs;
        rs = stat.executeQuery("select * from test");
        while (rs.next()) {
            System.out.println(rs.getInt(1) + " " + rs.getString(2));
        }
        System.out.println();
        System.out.println("test_view:");
        rs = stat.executeQuery("select * from test_view");
        while (rs.next()) {
            System.out.println(rs.getInt(1) + " " + rs.getString(2));
        }

        conn.close();
    }

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
            String tableName, boolean before, int type) throws SQLException {
        prepDelete = conn.prepareStatement("delete from test where id = ?");
        prepInsert = conn.prepareStatement("insert into test values(?, ?)");
        super.init(conn, schemaName, triggerName, tableName, before, type);
    }

    @Override
    public void fire(Connection conn, ResultSet oldRow, ResultSet newRow)
            throws SQLException {
        if (oldRow != null && oldRow.next()) {
            prepDelete.setInt(1, oldRow.getInt(1));
            prepDelete.execute();
        }
        if (newRow != null && newRow.next()) {
            prepInsert.setInt(1, newRow.getInt(1));
            prepInsert.setString(2, newRow.getString(2));
            prepInsert.execute();
        }
    }

}
