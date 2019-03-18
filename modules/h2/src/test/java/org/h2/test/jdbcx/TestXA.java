/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: James Devenish
 */
package org.h2.test.jdbcx;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.test.TestBase;
import org.h2.util.JdbcUtils;

/**
 * Basic XA tests.
 */
public class TestXA extends TestBase {

    private static final String DB_NAME1 = "xadb1";
    private static final String DB_NAME2 = "xadb2";

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
        testRollbackWithoutPrepare();
        testRollbackAfterPrepare();
        testXAAutoCommit();
        deleteDb("xa");
        testMixedXaNormal();
        testXA(true);
        deleteDb(DB_NAME1);
        deleteDb(DB_NAME2);
        testXA(false);
        deleteDb("xa");
        deleteDb(DB_NAME1);
        deleteDb(DB_NAME2);
    }

    private void testRollbackWithoutPrepare() throws Exception {
        if (config.memory) {
            return;
        }
        Xid xid = new Xid() {
            @Override
            public int getFormatId() {
                return 3145;
            }
            @Override
            public byte[] getGlobalTransactionId() {
                return new byte[] { 1, 2, 3, 4, 5, 6, 6, 7, 8 };
            }
            @Override
            public byte[] getBranchQualifier() {
                return new byte[] { 34, 43, 33, 3, 3, 3, 33, 33, 3 };
            }
        };
        deleteDb("xa");
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL(getURL("xa", true));
        ds.setPassword(getPassword());
        Connection dm = ds.getConnection();
        Statement stat = dm.createStatement();
        stat.execute("CREATE TABLE IF NOT EXISTS TEST(ID INT PRIMARY KEY, VAL INT)");
        stat.execute("INSERT INTO TEST(ID,VAL) VALUES (1,1)");
        dm.close();
        XAConnection c = ds.getXAConnection();
        XAResource xa = c.getXAResource();
        Connection connection = c.getConnection();
        xa.start(xid, XAResource.TMJOIN);
        PreparedStatement ps = connection.prepareStatement(
                "UPDATE TEST SET VAL=? WHERE ID=?");
        ps.setInt(1, new Random().nextInt());
        ps.setInt(2, 1);
        ps.close();
        xa.rollback(xid);
        connection.close();
        c.close();
        deleteDb("xa");
    }

    private void testRollbackAfterPrepare() throws Exception {
        if (config.memory) {
            return;
        }
        Xid xid = new Xid() {
            @Override
            public int getFormatId() {
                return 3145;
            }
            @Override
            public byte[] getGlobalTransactionId() {
                return new byte[] { 1, 2, 3, 4, 5, 6, 6, 7, 8 };
            }
            @Override
            public byte[] getBranchQualifier() {
                return new byte[] { 34, 43, 33, 3, 3, 3, 33, 33, 3 };
            }
        };
        deleteDb("xa");
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL(getURL("xa", true));
        ds.setPassword(getPassword());
        Connection dm = ds.getConnection();
        Statement stat = dm.createStatement();
        stat.execute("CREATE TABLE IF NOT EXISTS TEST(ID INT PRIMARY KEY, VAL INT)");
        stat.execute("INSERT INTO TEST(ID,VAL) VALUES (1,1)");
        dm.close();
        XAConnection c = ds.getXAConnection();
        XAResource xa = c.getXAResource();
        Connection connection = c.getConnection();
        xa.start(xid, XAResource.TMJOIN);
        PreparedStatement ps = connection.prepareStatement("UPDATE TEST SET VAL=? WHERE ID=?");
        ps.setInt(1, new Random().nextInt());
        ps.setInt(2, 1);
        ps.close();
        xa.prepare(xid);
        xa.rollback(xid);
        connection.close();
        c.close();
        deleteDb("xa");
    }


    private void testMixedXaNormal() throws Exception {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:test");
        ds.setUser("sa");
        ds.setPassword("");
        XAConnection xa = ds.getXAConnection();
        Connection c = xa.getConnection();
        assertTrue(c.getAutoCommit());
        MyXid xid = new MyXid();
        XAResource res = xa.getXAResource();

        res.start(xid, XAResource.TMNOFLAGS);
        assertTrue(!c.getAutoCommit());
        res.end(xid, XAResource.TMSUCCESS);
        res.commit(xid, true);
        assertTrue(c.getAutoCommit());

        res.start(xid, XAResource.TMNOFLAGS);
        assertTrue(!c.getAutoCommit());
        res.end(xid, XAResource.TMFAIL);
        res.rollback(xid);
        assertTrue(c.getAutoCommit());

        c.close();
        xa.close();
    }

    /**
     * A simple Xid implementation.
     */
    public static class MyXid implements Xid {
        private final byte[] branchQualifier = { 0 };
        private final byte[] globalTransactionId = { 0 };
        @Override
        public byte[] getBranchQualifier() {
            return branchQualifier;
        }
        @Override
        public int getFormatId() {
            return 0;
        }
        @Override
        public byte[] getGlobalTransactionId() {
            return globalTransactionId;
        }
    }

    private void testXAAutoCommit() throws Exception {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:test");
        ds.setUser("sa");
        ds.setPassword("");
        XAConnection xa = ds.getXAConnection();
        MyXid xid = new MyXid();
        xa.getXAResource().start(xid,
                XAResource.TMNOFLAGS);
        Connection c = xa.getConnection();
        assertTrue(!c.getAutoCommit());
        c.close();
        xa.close();
    }

    private void testXA(boolean useOneDatabase) throws SQLException {
        String url1 = getURL(DB_NAME1, true);
        String url2 = getURL(DB_NAME2, true);

        XAConnection xaConn1 = null;
        XAConnection xaConn2 = null;
        Connection conn1 = null;
        Connection conn2 = null;
        Statement stat1 = null;
        Statement stat2 = null;
        try {
            trace("xads1 = createXADatasource1()");
            XADataSource xaDs1 = createXADatasource(useOneDatabase, url1);
            trace("xads2 = createXADatasource2()");
            XADataSource xaDs2 = createXADatasource(useOneDatabase, url2);

            trace("xacon1 = xads1.getXAConnection()");
            xaConn1 = xaDs1.getXAConnection();
            trace("xacon2 = xads2.getXAConnection()");
            xaConn2 = xaDs2.getXAConnection();

            trace("xares1 = xacon1.getXAResource()");
            XAResource xares1 = xaConn1.getXAResource();
            trace("xares2 = xacon2.getXAResource()");
            XAResource xares2 = xaConn2.getXAResource();

            trace("xares1.recover(XAResource.TMSTARTRSCAN)");
            Xid[] xids1 = xares1.recover(XAResource.TMSTARTRSCAN);
            if ((xids1 == null) || (xids1.length == 0)) {
                trace("xares1.recover(XAResource.TMSTARTRSCAN): 0");
            } else {
                trace("xares1.recover(XAResource.TMSTARTRSCAN): " + xids1.length);
            }

            trace("xares2.recover(XAResource.TMSTARTRSCAN)");
            Xid[] xids2 = xares2.recover(XAResource.TMSTARTRSCAN);
            if ((xids2 == null) || (xids2.length == 0)) {
                trace("xares2.recover(XAResource.TMSTARTRSCAN): 0");
            } else {
                trace("xares2.recover(XAResource.TMSTARTRSCAN): " + xids2.length);
            }

            trace("con1 = xacon1.getConnection()");
            conn1 = xaConn1.getConnection();
            trace("stmt1 = con1.createStatement()");
            stat1 = conn1.createStatement();

            trace("con2 = xacon2.getConnection()");
            conn2 = xaConn2.getConnection();
            trace("stmt2 = con2.createStatement()");
            stat2 = conn2.createStatement();

            if (useOneDatabase) {
                trace("stmt1.executeUpdate(\"DROP TABLE xatest1\")");
                try {
                    stat1.executeUpdate("DROP TABLE xatest1");
                } catch (SQLException e) {
                    // ignore
                }
                trace("stmt2.executeUpdate(\"DROP TABLE xatest2\")");
                try {
                    stat2.executeUpdate("DROP TABLE xatest2");
                } catch (SQLException e) {
                    // ignore
                }
            } else {
                trace("stmt1.executeUpdate(\"DROP TABLE xatest\")");
                try {
                    stat1.executeUpdate("DROP TABLE xatest");
                } catch (SQLException e) {
                    // ignore
                }
                trace("stmt2.executeUpdate(\"DROP TABLE xatest\")");
                try {
                    stat2.executeUpdate("DROP TABLE xatest");
                } catch (SQLException e) {
                    // ignore
                }
            }

            if (useOneDatabase) {
                trace("stmt1.executeUpdate(\"CREATE TABLE xatest1 " +
                        "(id INT PRIMARY KEY, value INT)\")");
                stat1.executeUpdate("CREATE TABLE xatest1 " +
                        "(id INT PRIMARY KEY, value INT)");
                trace("stmt2.executeUpdate(\"CREATE TABLE xatest2 " +
                        "(id INT PRIMARY KEY, value INT)\")");
                stat2.executeUpdate("CREATE TABLE xatest2 " +
                        "(id INT PRIMARY KEY, value INT)");
            } else {
                trace("stmt1.executeUpdate(\"CREATE TABLE xatest " +
                        "(id INT PRIMARY KEY, value INT)\")");
                stat1.executeUpdate("CREATE TABLE xatest " +
                        "(id INT PRIMARY KEY, value INT)");
                trace("stmt2.executeUpdate(\"CREATE TABLE xatest " +
                        "(id INT PRIMARY KEY, value INT)\")");
                stat2.executeUpdate("CREATE TABLE xatest " +
                        "(id INT PRIMARY KEY, value INT)");
            }

            if (useOneDatabase) {
                trace("stmt1.executeUpdate(\"INSERT INTO xatest1 " +
                        "VALUES (1, 0)\")");
                stat1.executeUpdate("INSERT INTO xatest1 VALUES (1, 0)");
                trace("stmt2.executeUpdate(\"INSERT INTO xatest2 " +
                        "VALUES (2, 0)\")");
                stat2.executeUpdate("INSERT INTO xatest2 " +
                        "VALUES (2, 0)");
            } else {
                trace("stmt1.executeUpdate(\"INSERT INTO xatest " +
                        "VALUES (1, 0)\")");
                stat1.executeUpdate("INSERT INTO xatest " +
                        "VALUES (1, 0)");
                trace("stmt2.executeUpdate(\"INSERT INTO xatest " +
                        "VALUES (2, 0)\")");
                stat2.executeUpdate("INSERT INTO xatest " +
                        "VALUES (2, 0)");
            }

            Xid xid1 = null;
            Xid xid2 = null;

            if (useOneDatabase) {
                xid1 = SimpleXid.createRandom();
                xid2 = SimpleXid.createRandom();
            } else {
                xid1 = SimpleXid.createRandom();
                xid2 = xid1;
            }

            if (useOneDatabase) {
                trace("xares1.start(xid1, XAResource.TMNOFLAGS)");
                xares1.start(xid1, XAResource.TMNOFLAGS);
                trace("xares2.start(xid2, XAResource.TMJOIN)");
                xares2.start(xid2, XAResource.TMJOIN);
            } else {
                trace("xares1.start(xid1, XAResource.TMNOFLAGS)");
                xares1.start(xid1, XAResource.TMNOFLAGS);
                trace("xares2.start(xid2, XAResource.TMNOFLAGS)");
                xares2.start(xid2, XAResource.TMNOFLAGS);
            }

            if (useOneDatabase) {
                trace("stmt1.executeUpdate(\"UPDATE xatest1 " +
                        "SET value=1 WHERE id=1\")");
                stat1.executeUpdate("UPDATE xatest1 " +
                        "SET value=1 WHERE id=1");
                trace("stmt2.executeUpdate(\"UPDATE xatest2 " +
                        "SET value=1 WHERE id=2\")");
                stat2.executeUpdate("UPDATE xatest2 " +
                        "SET value=1 WHERE id=2");
            } else {
                trace("stmt1.executeUpdate(\"UPDATE xatest " +
                        "SET value=1 WHERE id=1\")");
                stat1.executeUpdate("UPDATE xatest " +
                        "SET value=1 WHERE id=1");
                trace("stmt2.executeUpdate(\"UPDATE xatest " +
                        "SET value=1 WHERE id=2\")");
                stat2.executeUpdate("UPDATE xatest " +
                        "SET value=1 WHERE id=2");
            }

            trace("xares1.end(xid1, XAResource.TMSUCCESS)");
            xares1.end(xid1, XAResource.TMSUCCESS);
            trace("xares2.end(xid2, XAResource.TMSUCCESS)");
            xares2.end(xid2, XAResource.TMSUCCESS);

            int ret1;
            int ret2;

            trace("ret1 = xares1.prepare(xid1)");
            ret1 = xares1.prepare(xid1);
            trace("xares1.prepare(xid1): " + ret1);
            trace("ret2 = xares2.prepare(xid2)");
            ret2 = xares2.prepare(xid2);
            trace("xares2.prepare(xid2): " + ret2);

            if ((ret1 != XAResource.XA_OK) && (ret1 != XAResource.XA_RDONLY)) {
                throw new IllegalStateException(
                        "xares1.prepare(xid1) must return XA_OK or XA_RDONLY");
            }
            if ((ret2 != XAResource.XA_OK) && (ret2 != XAResource.XA_RDONLY)) {
                throw new IllegalStateException(
                        "xares2.prepare(xid2) must return XA_OK or XA_RDONLY");
            }

            if (ret1 == XAResource.XA_OK) {
                trace("xares1.commit(xid1, false)");
                xares1.commit(xid1, false);
            }
            if (ret2 == XAResource.XA_OK) {
                trace("xares2.commit(xid2, false)");
                xares2.commit(xid2, false);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            JdbcUtils.closeSilently(stat1);
            JdbcUtils.closeSilently(stat2);
            JdbcUtils.closeSilently(conn1);
            JdbcUtils.closeSilently(conn2);
            if (xaConn1 != null) {
                xaConn1.close();
            }
            if (xaConn2 != null) {
                xaConn2.close();
            }
        }
    }

    private XADataSource createXADatasource(boolean useOneDatabase, String url) {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setPassword(getPassword(""));
        ds.setUser("sa");
        if (useOneDatabase) {
            ds.setURL(getURL("xa", true));
        } else {
            ds.setURL(url);
        }
        return ds;
    }

}
