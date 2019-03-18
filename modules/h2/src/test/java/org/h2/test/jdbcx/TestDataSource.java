/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.jdbcx;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.naming.spi.ObjectFactory;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.DataSource;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.h2.api.ErrorCode;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.jdbcx.JdbcDataSourceFactory;
import org.h2.jdbcx.JdbcXAConnection;
import org.h2.message.TraceSystem;
import org.h2.test.TestBase;

/**
 * Tests DataSource and XAConnection.
 */
public class TestDataSource extends TestBase {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

//     public static void main(String... args) throws SQLException {
//
//     // first, need to start on the command line:
//     // rmiregistry 1099
//
//     // System.setProperty(Context.INITIAL_CONTEXT_FACTORY,
//     "com.sun.jndi.ldap.LdapCtxFactory");
//     System.setProperty(Context.INITIAL_CONTEXT_FACTORY,
//     "com.sun.jndi.rmi.registry.RegistryContextFactory");
//     System.setProperty(Context.PROVIDER_URL, "rmi://localhost:1099");
//
//     JdbcDataSource ds = new JdbcDataSource();
//     ds.setURL("jdbc:h2:test");
//     ds.setUser("test");
//     ds.setPassword("");
//
//     Context ctx = new InitialContext();
//     ctx.bind("jdbc/test", ds);
//
//     DataSource ds2 = (DataSource)ctx.lookup("jdbc/test");
//     Connection conn = ds2.getConnection();
//     conn.close();
//     }

    @Override
    public void test() throws Exception {
        if (config.traceLevelFile > 0) {
            TraceSystem sys = JdbcDataSourceFactory.getTraceSystem();
            sys.setFileName(getBaseDir() + "/test/trace");
            sys.setLevelFile(3);
        }
        testDataSourceFactory();
        testDataSource();
        testUnwrap();
        testXAConnection();
        deleteDb("dataSource");
    }

    private void testDataSourceFactory() throws Exception {
        ObjectFactory factory = new JdbcDataSourceFactory();
        assertTrue(null == factory.getObjectInstance("test", null, null, null));
        Reference ref = new Reference("java.lang.String");
        assertTrue(null == factory.getObjectInstance(ref, null, null, null));
        ref = new Reference(JdbcDataSource.class.getName());
        ref.add(new StringRefAddr("url", "jdbc:h2:mem:"));
        ref.add(new StringRefAddr("user", "u"));
        ref.add(new StringRefAddr("password", "p"));
        ref.add(new StringRefAddr("loginTimeout", "1"));
        ref.add(new StringRefAddr("description", "test"));
        JdbcDataSource ds = (JdbcDataSource) factory.getObjectInstance(
                ref, null, null, null);
        assertEquals(1, ds.getLoginTimeout());
        assertEquals("test", ds.getDescription());
        assertEquals("jdbc:h2:mem:", ds.getURL());
        assertEquals("u", ds.getUser());
        assertEquals("p", ds.getPassword());
        Reference ref2 = ds.getReference();
        assertEquals(ref.size(), ref2.size());
        assertEquals(ref.get("url").getContent().toString(),
                ref2.get("url").getContent().toString());
        assertEquals(ref.get("user").getContent().toString(),
                ref2.get("user").getContent().toString());
        assertEquals(ref.get("password").getContent().toString(),
                ref2.get("password").getContent().toString());
        assertEquals(ref.get("loginTimeout").getContent().toString(),
                ref2.get("loginTimeout").getContent().toString());
        assertEquals(ref.get("description").getContent().toString(),
                ref2.get("description").getContent().toString());
        ds.setPasswordChars("abc".toCharArray());
        assertEquals("abc", ds.getPassword());
    }

    private void testXAConnection() throws Exception {
        testXAConnection(false);
        testXAConnection(true);
    }

    private void testXAConnection(boolean userInDataSource) throws Exception {
        deleteDb("dataSource");
        JdbcDataSource ds = new JdbcDataSource();
        String url = getURL("dataSource", true);
        String user = getUser();
        ds.setURL(url);
        if (userInDataSource) {
            ds.setUser(user);
            ds.setPassword(getPassword());
        }
        if (userInDataSource) {
            assertEquals("ds" + ds.getTraceId() + ": url=" + url +
                    " user=" + user, ds.toString());
        } else {
            assertEquals("ds" + ds.getTraceId() + ": url=" + url +
                    " user=", ds.toString());
        }
        XAConnection xaConn;
        if (userInDataSource) {
            xaConn = ds.getXAConnection();
        } else {
            xaConn = ds.getXAConnection(user, getPassword());
        }

        int traceId = ((JdbcXAConnection) xaConn).getTraceId();
        assertTrue(xaConn.toString().startsWith("xads" + traceId + ": conn"));

        xaConn.addConnectionEventListener(new ConnectionEventListener() {
            @Override
            public void connectionClosed(ConnectionEvent event) {
                // nothing to do
            }

            @Override
            public void connectionErrorOccurred(ConnectionEvent event) {
                // nothing to do
            }
        });
        XAResource res = xaConn.getXAResource();

        assertFalse(res.setTransactionTimeout(1));
        assertEquals(0, res.getTransactionTimeout());
        assertTrue(res.isSameRM(res));
        assertFalse(res.isSameRM(null));

        Connection conn = xaConn.getConnection();
        assertEquals(user.toUpperCase(), conn.getMetaData().getUserName());
        Xid[] list = res.recover(XAResource.TMSTARTRSCAN);
        assertEquals(0, list.length);
        Statement stat = conn.createStatement();
        stat.execute("SELECT * FROM DUAL");
        conn.close();
        xaConn.close();
    }

    private void testDataSource() throws SQLException {
        deleteDb("dataSource");
        JdbcDataSource ds = new JdbcDataSource();
        PrintWriter p = new PrintWriter(new StringWriter());
        ds.setLogWriter(p);
        assertTrue(p == ds.getLogWriter());
        ds.setURL(getURL("dataSource", true));
        ds.setUser(getUser());
        ds.setPassword(getPassword());
        Connection conn;
        conn = ds.getConnection();
        Statement stat;
        stat = conn.createStatement();
        stat.execute("SELECT * FROM DUAL");
        conn.close();
        conn = ds.getConnection(getUser(), getPassword());
        stat = conn.createStatement();
        stat.execute("SELECT * FROM DUAL");
        conn.close();
    }

    private void testUnwrap() throws SQLException {
        JdbcDataSource ds = new JdbcDataSource();
        assertTrue(ds.isWrapperFor(Object.class));
        assertTrue(ds.isWrapperFor(DataSource.class));
        assertTrue(ds.isWrapperFor(JdbcDataSource.class));
        assertFalse(ds.isWrapperFor(String.class));
        assertTrue(ds == ds.unwrap(Object.class));
        assertTrue(ds == ds.unwrap(DataSource.class));
        try {
            ds.unwrap(String.class);
            fail();
        } catch (SQLException ex) {
            assertEquals(ErrorCode.INVALID_VALUE_2, ex.getErrorCode());
        }
    }

}
