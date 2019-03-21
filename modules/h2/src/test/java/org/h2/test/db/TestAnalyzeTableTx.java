/*
 * Copyright 2004-2017 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import org.h2.test.TestBase;
import org.h2.test.TestDb;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestAnalyzeTableTx extends TestDb {
    private static final int C = 10_000;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public boolean isEnabled() {
        return !config.networked && !config.big;
    }

    @Override
    public void test() throws Exception {
        deleteDb(getTestName());
        Connection[] connections = new Connection[C];
        try (Connection shared = getConnection(getTestName())) {
            Statement statement = shared.createStatement();
            statement.executeUpdate("DROP TABLE IF EXISTS TEST");
            statement.executeUpdate("CREATE TABLE TEST(ID INT PRIMARY KEY)");
            for (int i = 0; i < C; i++) {
                Connection c = getConnection(getTestName());
                c.createStatement().executeUpdate("INSERT INTO TEST VALUES (" + i + ')');
                connections[i] = c;
            }
            try (ResultSet rs = statement.executeQuery("SELECT * FROM TEST")) {
                for (int i = 0; i < C; i++) {
                    if (!rs.next())
                        throw new Exception("next");
                    if (rs.getInt(1) != i)
                        throw new Exception(Integer.toString(i));
                }
            }
        } finally {
            for (Connection connection : connections) {
                if (connection != null) {
                    try { connection.close(); } catch (Throwable ignore) {/**/}
                }
            }
        }
    }
}
