/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import org.h2.api.ErrorCode;
import org.h2.test.TestBase;

/**
 * Transaction isolation level tests.
 */
public class TestTransactionIsolation extends TestBase {

    private Connection conn1, conn2;

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws SQLException {
        if (config.mvcc || config.mvStore) {
            // no tests yet
        } else {
            testTableLevelLocking();
        }
    }

    private void testTableLevelLocking() throws SQLException {
        deleteDb("transactionIsolation");
        conn1 = getConnection("transactionIsolation");
        assertEquals(Connection.TRANSACTION_READ_COMMITTED,
                conn1.getTransactionIsolation());
        conn1.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        assertEquals(Connection.TRANSACTION_SERIALIZABLE,
                conn1.getTransactionIsolation());
        conn1.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        assertEquals(Connection.TRANSACTION_READ_UNCOMMITTED,
                conn1.getTransactionIsolation());
        assertSingleValue(conn1.createStatement(), "CALL LOCK_MODE()", 0);
        conn1.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        assertSingleValue(conn1.createStatement(), "CALL LOCK_MODE()", 3);
        assertEquals(Connection.TRANSACTION_READ_COMMITTED,
                conn1.getTransactionIsolation());
        conn1.createStatement().execute("SET LOCK_MODE 1");
        assertEquals(Connection.TRANSACTION_SERIALIZABLE,
                conn1.getTransactionIsolation());
        conn1.createStatement().execute("CREATE TABLE TEST(ID INT)");
        conn1.createStatement().execute("INSERT INTO TEST VALUES(1)");
        conn1.setAutoCommit(false);

        conn2 = getConnection("transactionIsolation");
        conn2.setAutoCommit(false);

        conn1.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        // serializable: just reading
        assertSingleValue(conn1.createStatement(), "SELECT * FROM TEST", 1);
        assertSingleValue(conn2.createStatement(), "SELECT * FROM TEST", 1);
        conn1.commit();
        conn2.commit();

        // serializable: write lock
        conn1.createStatement().executeUpdate("UPDATE TEST SET ID=2");
        assertThrows(ErrorCode.LOCK_TIMEOUT_1, conn2.createStatement()).
                executeQuery("SELECT * FROM TEST");
        conn1.commit();
        conn2.commit();

        conn1.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

        // read-committed: #1 read, #2 update, #1 read again
        assertSingleValue(conn1.createStatement(), "SELECT * FROM TEST", 2);
        conn2.createStatement().executeUpdate("UPDATE TEST SET ID=3");
        conn2.commit();
        assertSingleValue(conn1.createStatement(), "SELECT * FROM TEST", 3);
        conn1.commit();

        // read-committed: #1 read, #2 read, #2 update, #1 delete
        assertSingleValue(conn1.createStatement(), "SELECT * FROM TEST", 3);
        assertSingleValue(conn2.createStatement(), "SELECT * FROM TEST", 3);
        conn2.createStatement().executeUpdate("UPDATE TEST SET ID=4");
        assertThrows(ErrorCode.LOCK_TIMEOUT_1, conn1.createStatement()).
                executeUpdate("DELETE FROM TEST");
        conn2.commit();
        conn1.commit();
        assertSingleValue(conn1.createStatement(), "SELECT * FROM TEST", 4);
        assertSingleValue(conn2.createStatement(), "SELECT * FROM TEST", 4);

        conn1.close();
        conn2.close();
        deleteDb("transactionIsolation");
    }

}
