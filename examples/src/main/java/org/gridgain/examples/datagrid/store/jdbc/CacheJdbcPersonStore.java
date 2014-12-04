/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.datagrid.store.jdbc;

import org.apache.ignite.lang.*;
import org.gridgain.examples.datagrid.store.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.store.*;
import org.jetbrains.annotations.*;

import java.sql.*;
import java.util.*;

/**
 * Example of {@link GridCacheStore} implementation that uses JDBC
 * transaction with cache transactions and maps {@link UUID} to {@link Person}.
 *
 */
public class CacheJdbcPersonStore extends GridCacheStoreAdapter<Long, Person> {
    /** Transaction metadata attribute name. */
    private static final String ATTR_NAME = "SIMPLE_STORE_CONNECTION";

    /**
     * Constructor.
     *
     * @throws GridException If failed.
     */
    public CacheJdbcPersonStore() throws GridException {
        prepareDb();
    }

    /**
     * Prepares database for example execution. This method will create a
     * table called "PERSONS" so it can be used by store implementation.
     *
     * @throws GridException If failed.
     */
    private void prepareDb() throws GridException {
        try (Connection conn = openConnection(false); Statement st = conn.createStatement()) {
            st.execute("create table if not exists PERSONS (id number unique, firstName varchar(255), " +
                "lastName varchar(255))");

            conn.commit();
        }
        catch (SQLException e) {
            throw new GridException("Failed to create database table.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void txEnd(GridCacheTx tx, boolean commit) throws GridException {
        try (Connection conn = tx.removeMeta(ATTR_NAME)) {
            if (conn != null) {
                if (commit)
                    conn.commit();
                else
                    conn.rollback();
            }

            System.out.println(">>> Transaction ended [xid=" + tx.xid() + ", commit=" + commit + ']');
        }
        catch (SQLException e) {
            throw new GridException("Failed to end transaction [xid=" + tx.xid() + ", commit=" + commit + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public Person load(@Nullable GridCacheTx tx, Long key) throws GridException {
        System.out.println(">>> Store load [key=" + key + ", xid=" + (tx == null ? null : tx.xid()) + ']');

        Connection conn = null;

        try {
            conn = connection(tx);

            try (PreparedStatement st = conn.prepareStatement("select * from PERSONS where id=?")) {
                st.setString(1, key.toString());

                ResultSet rs = st.executeQuery();

                if (rs.next())
                    return person(rs.getLong(1), rs.getString(2), rs.getString(3));
            }
        }
        catch (SQLException e) {
            throw new GridException("Failed to load object: " + key, e);
        }
        finally {
            end(tx, conn);
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void put(@Nullable GridCacheTx tx, Long key, Person val)
        throws GridException {
        System.out.println(">>> Store put [key=" + key + ", val=" + val + ", xid=" + (tx == null ? null : tx.xid()) + ']');

        Connection conn = null;

        try {
            conn = connection(tx);

        int updated;

        try (PreparedStatement st = conn.prepareStatement(
            "update PERSONS set firstName=?, lastName=? where id=?")) {
            st.setString(1, val.getFirstName());
            st.setString(2, val.getLastName());
            st.setLong(3, val.getId());

            updated = st.executeUpdate();
        }

        // If update failed, try to insert.
        if (updated == 0) {
            try (PreparedStatement st = conn.prepareStatement(
                "insert into PERSONS (id, firstName, lastName) values(?, ?, ?)")) {
                st.setLong(1, val.getId());
                st.setString(2, val.getFirstName());
                st.setString(3, val.getLastName());

                st.executeUpdate();
            }
        }
        }
        catch (SQLException e) {
            throw new GridException("Failed to put object [key=" + key + ", val=" + val + ']', e);
        }
        finally {
            end(tx, conn);
        }
    }

    /** {@inheritDoc} */
    @Override public void remove(@Nullable GridCacheTx tx, Long key) throws GridException {
        System.out.println(">>> Store remove [key=" + key + ", xid=" + (tx == null ? null : tx.xid()) + ']');

        Connection conn = null;

        try {
            conn = connection(tx);

            try (PreparedStatement st = conn.prepareStatement("delete from PERSONS where id=?")) {
                st.setLong(1, key);

                st.executeUpdate();
            }
        }
        catch (SQLException e) {
            throw new GridException("Failed to remove object: " + key, e);
        }
        finally {
            end(tx, conn);
        }
    }

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure<Long, Person> clo, Object... args) throws GridException {
        if (args == null || args.length == 0 || args[0] == null)
            throw new GridException("Expected entry count parameter is not provided.");

        final int entryCnt = (Integer)args[0];

        Connection conn = null;

        try {
            conn = connection(null);

            try (PreparedStatement st = conn.prepareStatement("select * from PERSONS")) {
                try (ResultSet rs = st.executeQuery()) {
                    int cnt = 0;

                    while (cnt < entryCnt && rs.next()) {
                        Person person = person(rs.getLong(1), rs.getString(2), rs.getString(3));

                        clo.apply(person.getId(), person);

                        cnt++;
                    }

                    System.out.println(">>> Loaded " + cnt + " values into cache.");
                }
            }
        }
        catch (SQLException e) {
            throw new GridException("Failed to load values from cache store.", e);
        }
        finally {
            end(null, conn);
        }
    }

    /**
     * @param tx Cache transaction.
     * @return Connection.
     * @throws SQLException In case of error.
     */
    private Connection connection(@Nullable GridCacheTx tx) throws SQLException  {
        if (tx != null) {
            Connection conn = tx.meta(ATTR_NAME);

            if (conn == null) {
                conn = openConnection(false);

                // Store connection in transaction metadata, so it can be accessed
                // for other operations on the same transaction.
                tx.addMeta(ATTR_NAME, conn);
            }

            return conn;
        }
        // Transaction can be null in case of simple load or put operation.
        else
            return openConnection(true);
    }

    /**
     * Closes allocated resources depending on transaction status.
     *
     * @param tx Active transaction, if any.
     * @param conn Allocated connection.
     */
    private void end(@Nullable GridCacheTx tx, @Nullable Connection conn) {
        if (tx == null && conn != null) {
            // Close connection right away if there is no transaction.
            try {
                conn.close();
            }
            catch (SQLException ignored) {
                // No-op.
            }
        }
    }

    /**
     * Gets connection from a pool.
     *
     * @param autocommit {@code true} If connection should use autocommit mode.
     * @return Pooled connection.
     * @throws SQLException In case of error.
     */
    private Connection openConnection(boolean autocommit) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:h2:mem:example;DB_CLOSE_DELAY=-1");

        conn.setAutoCommit(autocommit);

        return conn;
    }

    /**
     * Builds person object out of provided values.
     *
     * @param id ID.
     * @param firstName First name.
     * @param lastName Last name.
     * @return Person.
     */
    private Person person(Long id, String firstName, String lastName) {
        return new Person(id, firstName, lastName);
    }
}
