/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.examples.datagrid.store.jdbc;

import org.apache.ignite.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.examples.datagrid.store.model.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.integration.*;
import java.sql.*;
import java.util.*;

/**
 * Example of {@link CacheStore} implementation that uses JDBC
 * transaction with cache transactions and maps {@link Long} to {@link Person}.
 *
 */
public class CacheJdbcPersonStore extends CacheStoreAdapter<Long, Person> {
    /** Transaction metadata attribute name. */
    private static final String ATTR_NAME = "SIMPLE_STORE_CONNECTION";

    /** Auto-injected store session. */
    @CacheStoreSessionResource
    private CacheStoreSession ses;

    /**
     * Constructor.
     *
     * @throws IgniteException If failed.
     */
    public CacheJdbcPersonStore() throws IgniteException {
        prepareDb();
    }

    /**
     * Prepares database for example execution. This method will create a
     * table called "PERSONS" so it can be used by store implementation.
     *
     * @throws IgniteException If failed.
     */
    private void prepareDb() throws IgniteException {
        try (Connection conn = openConnection(false); Statement st = conn.createStatement()) {
            st.execute("create table if not exists PERSONS (id number unique, firstName varchar(255), " +
                "lastName varchar(255))");

            conn.commit();
        }
        catch (SQLException e) {
            throw new IgniteException("Failed to create database table.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void txEnd(boolean commit) {
        Map<String, Connection> props = ses.properties();

        try (Connection conn = props.remove(ATTR_NAME)) {
            if (conn != null) {
                if (commit)
                    conn.commit();
                else
                    conn.rollback();
            }

            System.out.println(">>> Transaction ended [commit=" + commit + ']');
        }
        catch (SQLException e) {
            throw new CacheWriterException("Failed to end transaction: " + ses.transaction(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public Person load(Long key) {
        System.out.println(">>> Loading key: " + key);

        Connection conn = null;

        try {
            conn = connection();

            try (PreparedStatement st = conn.prepareStatement("select * from PERSONS where id=?")) {
                st.setString(1, key.toString());

                ResultSet rs = st.executeQuery();

                if (rs.next())
                    return new Person(rs.getLong(1), rs.getString(2), rs.getString(3));
            }
        }
        catch (SQLException e) {
            throw new CacheLoaderException("Failed to load object: " + key, e);
        }
        finally {
            end(conn);
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void write(Cache.Entry<? extends Long, ? extends Person> entry) {
        Long key = entry.getKey();

        Person val = entry.getValue();

        System.out.println(">>> Putting [key=" + key + ", val=" + val +  ']');

        Connection conn = null;

        try {
            conn = connection();

            int updated;

            // Try update first. If it does not work, then try insert.
            // Some databases would allow these to be done in one 'upsert' operation.
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
            throw new CacheLoaderException("Failed to put object [key=" + key + ", val=" + val + ']', e);
        }
        finally {
            end(conn);
        }
    }

    /** {@inheritDoc} */
    @Override public void delete(Object key) {
        System.out.println(">>> Removing key: " + key);

        Connection conn = null;

        try {
            conn = connection();

            try (PreparedStatement st = conn.prepareStatement("delete from PERSONS where id=?")) {
                st.setLong(1, (Long)key);

                st.executeUpdate();
            }
        }
        catch (SQLException e) {
            throw new CacheWriterException("Failed to remove object: " + key, e);
        }
        finally {
            end(conn);
        }
    }

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure<Long, Person> clo, Object... args) {
        if (args == null || args.length == 0 || args[0] == null)
            throw new CacheLoaderException("Expected entry count parameter is not provided.");

        final int entryCnt = (Integer)args[0];

        try (Connection conn = connection()) {
            try (PreparedStatement st = conn.prepareStatement("select * from PERSONS")) {
                try (ResultSet rs = st.executeQuery()) {
                    int cnt = 0;

                    while (cnt < entryCnt && rs.next()) {
                        Person person = new Person(rs.getLong(1), rs.getString(2), rs.getString(3));

                        clo.apply(person.getId(), person);

                        cnt++;
                    }

                    System.out.println(">>> Loaded " + cnt + " values into cache.");
                }
            }
        }
        catch (SQLException e) {
            throw new CacheLoaderException("Failed to load values from cache store.", e);
        }
    }

    /**
     * @return Connection.
     * @throws SQLException In case of error.
     */
    private Connection connection() throws SQLException  {
        // If there is an ongoing transaction,
        // we must reuse the same connection.
        if (ses.isWithinTransaction()) {
            Map<Object, Object> props = ses.properties();

            Connection conn = (Connection)props.get(ATTR_NAME);

            if (conn == null) {
                conn = openConnection(false);

                // Store connection in session properties, so it can be accessed
                // for other operations on the same transaction.
                props.put(ATTR_NAME, conn);
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
     * @param conn Allocated connection.
     */
    private void end(@Nullable Connection conn) {
        if (!ses.isWithinTransaction() && conn != null) {
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
}
