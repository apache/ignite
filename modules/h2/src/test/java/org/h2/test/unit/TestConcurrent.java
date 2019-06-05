/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.unit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.api.ErrorCode;
import org.h2.test.TestBase;
import org.h2.util.Task;

/**
 * Test concurrent access to JDBC objects.
 */
public class TestConcurrent extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        String url = "jdbc:h2:mem:";
        for (int i = 0; i < 50; i++) {
            final int x = i % 4;
            final Connection conn = DriverManager.getConnection(url);
            final Statement stat = conn.createStatement();
            stat.execute("create table test(id int primary key)");
            String sql = "";
            switch (x % 6) {
            case 0:
                sql = "select 1";
                break;
            case 1:
            case 2:
                sql = "delete from test";
                break;
            }
            final PreparedStatement prep = conn.prepareStatement(sql);
            Task t = new Task() {
                @Override
                public void call() throws SQLException {
                    while (!conn.isClosed()) {
                        switch (x % 6) {
                        case 0:
                            prep.executeQuery();
                            break;
                        case 1:
                            prep.execute();
                            break;
                        case 2:
                            prep.executeUpdate();
                            break;
                        case 3:
                            stat.executeQuery("select 1");
                            break;
                        case 4:
                            stat.execute("select 1");
                            break;
                        case 5:
                            stat.execute("delete from test");
                            break;
                        }
                    }
                }
            };
            t.execute();
            Thread.sleep(100);
            conn.close();
            SQLException e = (SQLException) t.getException();
            if (e != null) {
                if (ErrorCode.OBJECT_CLOSED != e.getErrorCode() &&
                        ErrorCode.STATEMENT_WAS_CANCELED != e.getErrorCode()) {
                    throw e;
                }
            }
        }
    }

}
