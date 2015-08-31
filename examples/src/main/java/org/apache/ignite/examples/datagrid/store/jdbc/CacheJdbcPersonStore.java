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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.sql.DataSource;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.examples.datagrid.store.Person;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.h2.jdbcx.JdbcConnectionPool;

/**
 * Example of {@link CacheStore} implementation that uses JDBC
 * transaction with cache transactions and maps {@link Long} to {@link Person}.
 */
public class CacheJdbcPersonStore extends CacheStoreAdapter<Long, Person> {
    /** Data source. */
    public static final DataSource DATA_SRC =
        JdbcConnectionPool.create("jdbc:h2:mem:example;DB_CLOSE_DELAY=-1", "", "");

    /** Store session. */
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
        try (Connection conn = DATA_SRC.getConnection()) {
            conn.createStatement().execute(
                "create table if not exists PERSONS (" +
                "id number unique, firstName varchar(255), lastName varchar(255))");
        }
        catch (SQLException e) {
            throw new IgniteException("Failed to create database table.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public Person load(Long key) {
        System.out.println(">>> Store load [key=" + key + ']');

        Connection conn = ses.attachment();

        try (PreparedStatement st = conn.prepareStatement("select * from PERSONS where id = ?")) {
            st.setString(1, key.toString());

            ResultSet rs = st.executeQuery();

            return rs.next() ? new Person(rs.getLong(1), rs.getString(2), rs.getString(3)) : null;
        }
        catch (SQLException e) {
            throw new CacheLoaderException("Failed to load object [key=" + key + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public void write(Cache.Entry<? extends Long, ? extends Person> entry) {
        Long key = entry.getKey();
        Person val = entry.getValue();

        System.out.println(">>> Store write [key=" + key + ", val=" + val + ']');

        try {
            Connection conn = ses.attachment();

            int updated;

            // Try update first. If it does not work, then try insert.
            // Some databases would allow these to be done in one 'upsert' operation.
            try (PreparedStatement st = conn.prepareStatement(
                "update PERSONS set firstName = ?, lastName = ? where id = ?")) {
                st.setString(1, val.getFirstName());
                st.setString(2, val.getLastName());
                st.setLong(3, val.getId());

                updated = st.executeUpdate();
            }

            // If update failed, try to insert.
            if (updated == 0) {
                try (PreparedStatement st = conn.prepareStatement(
                    "insert into PERSONS (id, firstName, lastName) values (?, ?, ?)")) {
                    st.setLong(1, val.getId());
                    st.setString(2, val.getFirstName());
                    st.setString(3, val.getLastName());

                    st.executeUpdate();
                }
            }
        }
        catch (SQLException e) {
            throw new CacheWriterException("Failed to write object [key=" + key + ", val=" + val + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public void delete(Object key) {
        System.out.println(">>> Store delete [key=" + key + ']');

        Connection conn = ses.attachment();

        try (PreparedStatement st = conn.prepareStatement("delete from PERSONS where id=?")) {
            st.setLong(1, (Long)key);

            st.executeUpdate();
        }
        catch (SQLException e) {
            throw new CacheWriterException("Failed to delete object [key=" + key + ']', e);
        }
    }

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure<Long, Person> clo, Object... args) {
        if (args == null || args.length == 0 || args[0] == null)
            throw new CacheLoaderException("Expected entry count parameter is not provided.");

        final int entryCnt = (Integer)args[0];

        Connection conn = ses.attachment();

        try (PreparedStatement stmt = conn.prepareStatement("select * from PERSONS limit ?")) {
            stmt.setInt(1, entryCnt);

            ResultSet rs = stmt.executeQuery();

            int cnt = 0;

            while (rs.next()) {
                Person person = new Person(rs.getLong(1), rs.getString(2), rs.getString(3));

                clo.apply(person.getId(), person);

                cnt++;
            }

            System.out.println(">>> Loaded " + cnt + " values into cache.");
        }
        catch (SQLException e) {
            throw new CacheLoaderException("Failed to load values from cache store.", e);
        }
    }
}