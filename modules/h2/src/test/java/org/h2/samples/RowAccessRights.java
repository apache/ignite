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
import org.h2.tools.DeleteDbFiles;
import org.h2.tools.TriggerAdapter;

/**
 * This sample application show how to emulate per-row access rights so that
 * each user can only access rows created by the given user.
 */
public class RowAccessRights extends TriggerAdapter {

    private PreparedStatement prepDelete, prepInsert;

    /**
     * Called when ran from command line.
     *
     * @param args ignored
     */
    public static void main(String... args) throws Exception {
        DeleteDbFiles.execute("~", "test", true);

        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection(
                "jdbc:h2:~/test");
        Statement stat = conn.createStatement();

        stat.execute("create table test_data(" +
                "id int, user varchar, data varchar, primary key(id, user))");
        stat.execute("create index on test_data(id, user)");

        stat.execute("create view test as select id, data " +
                "from test_data where user = user()");
        stat.execute("create trigger t_test instead of " +
                "insert, update, delete on test for each row " +
                "call \"" + RowAccessRights.class.getName() + "\"");
        stat.execute("create user a password 'a'");
        stat.execute("create user b password 'b'");
        stat.execute("grant all on test to a");
        stat.execute("grant all on test to b");

        ResultSet rs;

        Connection connA = DriverManager.getConnection(
                "jdbc:h2:~/test", "a", "a");
        Statement statA = connA.createStatement();
        statA.execute("insert into test values(1, 'Hello'), (2, 'World')");
        statA.execute("update test set data = 'Hello!' where id = 1");
        statA.execute("delete from test where id = 2");

        Connection connB = DriverManager.getConnection(
                "jdbc:h2:~/test", "b", "b");
        Statement statB = connB.createStatement();
        statB.execute("insert into test values(1, 'Hallo'), (2, 'Welt')");
        statB.execute("update test set data = 'Hallo!' where id = 1");
        statB.execute("delete from test where id = 2");

        rs = statA.executeQuery("select * from test");
        while (rs.next()) {
            System.out.println("a: " + rs.getInt(1) + "/" + rs.getString(2));
        }

        rs = statB.executeQuery("select * from test");
        while (rs.next()) {
            System.out.println("b: " +
                    rs.getInt(1) + "/" + rs.getString(2));
        }

        connA.close();
        connB.close();

        rs = stat.executeQuery("select * from test_data");
        while (rs.next()) {
            System.out.println(rs.getInt(1) + "/" +
                    rs.getString(2) + "/" + rs.getString(3));
        }
        conn.close();

    }

    @Override
    public void init(Connection conn, String schemaName, String triggerName,
            String tableName, boolean before, int type) throws SQLException {
        prepDelete = conn.prepareStatement(
                "delete from test_data where id = ? and user = ?");
        prepInsert = conn.prepareStatement(
                "insert into test_data values(?, ?, ?)");
        super.init(conn, schemaName, triggerName, tableName, before, type);
    }

    @Override
    public void fire(Connection conn, ResultSet oldRow, ResultSet newRow)
            throws SQLException {
        String user = conn.getMetaData().getUserName();
        if (oldRow != null && oldRow.next()) {
            prepDelete.setInt(1, oldRow.getInt(1));
            prepDelete.setString(2, user);
            int deleted = prepDelete.executeUpdate();
            if (deleted == 0 && newRow != null) {
                // update:
                // if deleting failed, insert must fail as well
                newRow = null;
            }
        }
        if (newRow != null && newRow.next()) {
            prepInsert.setInt(1, newRow.getInt(1));
            prepInsert.setString(2, user);
            prepInsert.setString(3, newRow.getString(2));
            prepInsert.executeUpdate();
        }
    }

}
