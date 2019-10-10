/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jdbcx;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.h2.api.ErrorCode;
import org.h2.jdbc.JdbcConnection;
import org.h2.message.DbException;
import org.h2.message.TraceObject;
import org.h2.util.New;


/**
 * This class provides support for distributed transactions.
 * An application developer usually does not use this interface.
 * It is used by the transaction manager internally.
 */
public class JdbcXAConnection extends TraceObject implements XAConnection,
        XAResource {

    private final JdbcDataSourceFactory factory;

    // this connection is kept open as long as the XAConnection is alive
    private JdbcConnection physicalConn;

    // this connection is replaced whenever getConnection is called
    private volatile Connection handleConn;
    private final ArrayList<ConnectionEventListener> listeners = New.arrayList();
    private Xid currentTransaction;
    private boolean prepared;

    static {
        org.h2.Driver.load();
    }

    JdbcXAConnection(JdbcDataSourceFactory factory, int id,
            JdbcConnection physicalConn) {
        this.factory = factory;
        setTrace(factory.getTrace(), TraceObject.XA_DATA_SOURCE, id);
        this.physicalConn = physicalConn;
    }

    /**
     * Get the XAResource object.
     *
     * @return itself
     */
    @Override
    public XAResource getXAResource() {
        debugCodeCall("getXAResource");
        return this;
    }

    /**
     * Close the physical connection.
     * This method is usually called by the connection pool.
     */
    @Override
    public void close() throws SQLException {
        debugCodeCall("close");
        Connection lastHandle = handleConn;
        if (lastHandle != null) {
            listeners.clear();
            lastHandle.close();
        }
        if (physicalConn != null) {
            try {
                physicalConn.close();
            } finally {
                physicalConn = null;
            }
        }
    }

    /**
     * Get a connection that is a handle to the physical connection. This method
     * is usually called by the connection pool. This method closes the last
     * connection handle if one exists.
     *
     * @return the connection
     */
    @Override
    public Connection getConnection() throws SQLException {
        debugCodeCall("getConnection");
        Connection lastHandle = handleConn;
        if (lastHandle != null) {
            lastHandle.close();
        }
        // this will ensure the rollback command is cached
        physicalConn.rollback();
        handleConn = new PooledJdbcConnection(physicalConn);
        return handleConn;
    }

    /**
     * Register a new listener for the connection.
     *
     * @param listener the event listener
     */
    @Override
    public void addConnectionEventListener(ConnectionEventListener listener) {
        debugCode("addConnectionEventListener(listener);");
        listeners.add(listener);
    }

    /**
     * Remove the event listener.
     *
     * @param listener the event listener
     */
    @Override
    public void removeConnectionEventListener(ConnectionEventListener listener) {
        debugCode("removeConnectionEventListener(listener);");
        listeners.remove(listener);
    }

    /**
     * INTERNAL
     */
    void closedHandle() {
        debugCode("closedHandle();");
        ConnectionEvent event = new ConnectionEvent(this);
        // go backward so that a listener can remove itself
        // (otherwise we need to clone the list)
        for (int i = listeners.size() - 1; i >= 0; i--) {
            ConnectionEventListener listener = listeners.get(i);
            listener.connectionClosed(event);
        }
        handleConn = null;
    }

    /**
     * Get the transaction timeout.
     *
     * @return 0
     */
    @Override
    public int getTransactionTimeout() {
        debugCodeCall("getTransactionTimeout");
        return 0;
    }

    /**
     * Set the transaction timeout.
     *
     * @param seconds ignored
     * @return false
     */
    @Override
    public boolean setTransactionTimeout(int seconds) {
        debugCodeCall("setTransactionTimeout", seconds);
        return false;
    }

    /**
     * Checks if this is the same XAResource.
     *
     * @param xares the other object
     * @return true if this is the same object
     */
    @Override
    public boolean isSameRM(XAResource xares) {
        debugCode("isSameRM(xares);");
        return xares == this;
    }

    /**
     * Get the list of prepared transaction branches. This method is called by
     * the transaction manager during recovery.
     *
     * @param flag TMSTARTRSCAN, TMENDRSCAN, or TMNOFLAGS. If no other flags are
     *            set, TMNOFLAGS must be used.
     * @return zero or more Xid objects
     */
    @Override
    public Xid[] recover(int flag) throws XAException {
        debugCodeCall("recover", quoteFlags(flag));
        checkOpen();
        try (Statement stat = physicalConn.createStatement()) {
            ResultSet rs = stat.executeQuery("SELECT * FROM " +
                    "INFORMATION_SCHEMA.IN_DOUBT ORDER BY TRANSACTION");
            ArrayList<Xid> list = New.arrayList();
            while (rs.next()) {
                String tid = rs.getString("TRANSACTION");
                int id = getNextId(XID);
                Xid xid = new JdbcXid(factory, id, tid);
                list.add(xid);
            }
            rs.close();
            Xid[] result = list.toArray(new Xid[0]);
            if (!list.isEmpty()) {
                prepared = true;
            }
            return result;
        } catch (SQLException e) {
            XAException xa = new XAException(XAException.XAER_RMERR);
            xa.initCause(e);
            throw xa;
        }
    }

    /**
     * Prepare a transaction.
     *
     * @param xid the transaction id
     * @return XA_OK
     */
    @Override
    public int prepare(Xid xid) throws XAException {
        if (isDebugEnabled()) {
            debugCode("prepare("+JdbcXid.toString(xid)+");");
        }
        checkOpen();
        if (!currentTransaction.equals(xid)) {
            throw new XAException(XAException.XAER_INVAL);
        }

        try (Statement stat = physicalConn.createStatement()) {
            stat.execute("PREPARE COMMIT " + JdbcXid.toString(xid));
            prepared = true;
        } catch (SQLException e) {
            throw convertException(e);
        }
        return XA_OK;
    }

    /**
     * Forget a transaction.
     * This method does not have an effect for this database.
     *
     * @param xid the transaction id
     */
    @Override
    public void forget(Xid xid) {
        if (isDebugEnabled()) {
            debugCode("forget("+JdbcXid.toString(xid)+");");
        }
        prepared = false;
    }

    /**
     * Roll back a transaction.
     *
     * @param xid the transaction id
     */
    @Override
    public void rollback(Xid xid) throws XAException {
        if (isDebugEnabled()) {
            debugCode("rollback("+JdbcXid.toString(xid)+");");
        }
        try {
            if (prepared) {
                try (Statement stat = physicalConn.createStatement()) {
                    stat.execute("ROLLBACK TRANSACTION " + JdbcXid.toString(xid));
                }
                prepared = false;
            } else {
                physicalConn.rollback();
            }
            physicalConn.setAutoCommit(true);
        } catch (SQLException e) {
            throw convertException(e);
        }
        currentTransaction = null;
    }

    /**
     * End a transaction.
     *
     * @param xid the transaction id
     * @param flags TMSUCCESS, TMFAIL, or TMSUSPEND
     */
    @Override
    public void end(Xid xid, int flags) throws XAException {
        if (isDebugEnabled()) {
            debugCode("end("+JdbcXid.toString(xid)+", "+quoteFlags(flags)+");");
        }
        // TODO transaction end: implement this method
        if (flags == TMSUSPEND) {
            return;
        }
        if (!currentTransaction.equals(xid)) {
            throw new XAException(XAException.XAER_OUTSIDE);
        }
        prepared = false;
    }

    /**
     * Start or continue to work on a transaction.
     *
     * @param xid the transaction id
     * @param flags TMNOFLAGS, TMJOIN, or TMRESUME
     */
    @Override
    public void start(Xid xid, int flags) throws XAException {
        if (isDebugEnabled()) {
            debugCode("start("+JdbcXid.toString(xid)+", "+quoteFlags(flags)+");");
        }
        if (flags == TMRESUME) {
            return;
        }
        if (flags == TMJOIN) {
            if (currentTransaction != null && !currentTransaction.equals(xid)) {
                throw new XAException(XAException.XAER_RMERR);
            }
        } else if (currentTransaction != null) {
            throw new XAException(XAException.XAER_NOTA);
        }
        try {
            physicalConn.setAutoCommit(false);
        } catch (SQLException e) {
            throw convertException(e);
        }
        currentTransaction = xid;
        prepared = false;
    }

    /**
     * Commit a transaction.
     *
     * @param xid the transaction id
     * @param onePhase use a one-phase protocol if true
     */
    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
        if (isDebugEnabled()) {
            debugCode("commit("+JdbcXid.toString(xid)+", "+onePhase+");");
        }

        try {
            if (onePhase) {
                physicalConn.commit();
            } else {
                try (Statement stat = physicalConn.createStatement()) {
                    stat.execute("COMMIT TRANSACTION " + JdbcXid.toString(xid));
                    prepared = false;
                }
            }
            physicalConn.setAutoCommit(true);
        } catch (SQLException e) {
            throw convertException(e);
        }
        currentTransaction = null;
    }

    /**
     * [Not supported] Add a statement event listener.
     *
     * @param listener the new statement event listener
     */
    @Override
    public void addStatementEventListener(StatementEventListener listener) {
        throw new UnsupportedOperationException();
    }

    /**
     * [Not supported] Remove a statement event listener.
     *
     * @param listener the statement event listener
     */
    @Override
    public void removeStatementEventListener(StatementEventListener listener) {
        throw new UnsupportedOperationException();
    }

    /**
     * INTERNAL
     */
    @Override
    public String toString() {
        return getTraceObjectName() + ": " + physicalConn;
    }

    private static XAException convertException(SQLException e) {
        XAException xa = new XAException(e.getMessage());
        xa.initCause(e);
        return xa;
    }

    private static String quoteFlags(int flags) {
        StringBuilder buff = new StringBuilder();
        if ((flags & XAResource.TMENDRSCAN) != 0) {
            buff.append("|XAResource.TMENDRSCAN");
        }
        if ((flags & XAResource.TMFAIL) != 0) {
            buff.append("|XAResource.TMFAIL");
        }
        if ((flags & XAResource.TMJOIN) != 0) {
            buff.append("|XAResource.TMJOIN");
        }
        if ((flags & XAResource.TMONEPHASE) != 0) {
            buff.append("|XAResource.TMONEPHASE");
        }
        if ((flags & XAResource.TMRESUME) != 0) {
            buff.append("|XAResource.TMRESUME");
        }
        if ((flags & XAResource.TMSTARTRSCAN) != 0) {
            buff.append("|XAResource.TMSTARTRSCAN");
        }
        if ((flags & XAResource.TMSUCCESS) != 0) {
            buff.append("|XAResource.TMSUCCESS");
        }
        if ((flags & XAResource.TMSUSPEND) != 0) {
            buff.append("|XAResource.TMSUSPEND");
        }
        if ((flags & XAResource.XA_RDONLY) != 0) {
            buff.append("|XAResource.XA_RDONLY");
        }
        if (buff.length() == 0) {
            buff.append("|XAResource.TMNOFLAGS");
        }
        return buff.toString().substring(1);
    }

    private void checkOpen() throws XAException {
        if (physicalConn == null) {
            throw new XAException(XAException.XAER_RMERR);
        }
    }

    /**
     * A pooled connection.
     */
    class PooledJdbcConnection extends JdbcConnection {

        private boolean isClosed;

        public PooledJdbcConnection(JdbcConnection conn) {
            super(conn);
        }

        @Override
        public synchronized void close() throws SQLException {
            if (!isClosed) {
                try {
                    rollback();
                    setAutoCommit(true);
                } catch (SQLException e) {
                    // ignore
                }
                closedHandle();
                isClosed = true;
            }
        }

        @Override
        public synchronized boolean isClosed() throws SQLException {
            return isClosed || super.isClosed();
        }

        @Override
        protected synchronized void checkClosed(boolean write) {
            if (isClosed) {
                throw DbException.get(ErrorCode.OBJECT_CLOSED);
            }
            super.checkClosed(write);
        }

    }

}
