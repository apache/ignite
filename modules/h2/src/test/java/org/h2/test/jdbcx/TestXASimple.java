/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbcx;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.test.TestBase;
import org.h2.util.JdbcUtils;

/**
 * A simple XA test.
 */
public class TestXASimple extends TestBase {

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
        testTwoPhase();
        testSimple();
    }

    private void testTwoPhase() throws Exception {
        if (config.memory || config.networked) {
            return;
        }
        // testTwoPhase(false, true);
        // testTwoPhase(false, false);
        testTwoPhase("xaSimple2a", true, true);
        testTwoPhase("xaSimple2b", true, false);

    }

    private void testTwoPhase(String db, boolean shutdown, boolean commit)
            throws Exception {
        deleteDb(db);
        JdbcDataSource ds = new JdbcDataSource();
        ds.setPassword(getPassword());
        ds.setUser("sa");
        // ds.setURL(getURL("xaSimple", true) + ";trace_level_system_out=3");
        ds.setURL(getURL(db, true));

        XAConnection xa;
        xa = ds.getXAConnection();
        Connection conn;

        conn = xa.getConnection();
        Statement stat = conn.createStatement();
        stat.execute("create table test(id int primary key, name varchar(255))");
        Xid xid = SimpleXid.createRandom();
        xa.getXAResource().start(xid, XAResource.TMNOFLAGS);
        conn.setAutoCommit(false);
        stat.execute("insert into test values(1, 'Hello')");
        xa.getXAResource().end(xid, XAResource.TMSUCCESS);
        xa.getXAResource().prepare(xid);
        if (shutdown) {
            shutdown(ds);
        }

        xa = ds.getXAConnection();
        Xid[] list = xa.getXAResource().recover(XAResource.TMSTARTRSCAN);
        assertEquals(1, list.length);
        assertTrue(xid.equals(list[0]));
        if (commit) {
            xa.getXAResource().commit(list[0], false);
        } else {
            xa.getXAResource().rollback(list[0]);
        }
        conn = xa.getConnection();
        conn.createStatement().executeQuery("select * from test");
        if (shutdown) {
            shutdown(ds);
        }

        xa = ds.getXAConnection();
        list = xa.getXAResource().recover(XAResource.TMSTARTRSCAN);
        assertEquals(0, list.length);
        conn = xa.getConnection();
        ResultSet rs;
        rs = conn.createStatement().executeQuery("select * from test");
        if (commit) {
            assertTrue(rs.next());
        } else {
            assertFalse(rs.next());
        }
        xa.close();
    }

    private static void shutdown(JdbcDataSource ds) throws SQLException {
        Connection conn = ds.getConnection();
        conn.createStatement().execute("shutdown immediately");
        JdbcUtils.closeSilently(conn);
    }

    private void testSimple() throws SQLException {

        deleteDb("xaSimple1");
        deleteDb("xaSimple2");
        org.h2.Driver.load();

        // InitialContext context = new InitialContext();
        // context.rebind(USER_TRANSACTION_JNDI_NAME, j.getUserTransaction());

        JdbcDataSource ds1 = new JdbcDataSource();
        ds1.setPassword(getPassword());
        ds1.setUser("sa");
        ds1.setURL(getURL("xaSimple1", true));

        JdbcDataSource ds2 = new JdbcDataSource();
        ds2.setPassword(getPassword());
        ds2.setUser("sa");
        ds2.setURL(getURL("xaSimple2", true));

        // UserTransaction ut = (UserTransaction)
        // context.lookup("UserTransaction");
        // ut.begin();

        XAConnection xa1 = ds1.getXAConnection();
        Connection c1 = xa1.getConnection();
        c1.setAutoCommit(false);
        XAConnection xa2 = ds2.getXAConnection();
        Connection c2 = xa2.getConnection();
        c2.setAutoCommit(false);

        c1.createStatement().executeUpdate(
                "create table test(id int, test varchar(255))");
        c2.createStatement().executeUpdate(
                "create table test(id int, test varchar(255))");

        // ut.rollback();
        c1.close();
        c2.close();

        xa1.close();
        xa2.close();

        // j.stop();
        // System.exit(0);
        deleteDb("xaSimple1");
        deleteDb("xaSimple2");

    }

}
