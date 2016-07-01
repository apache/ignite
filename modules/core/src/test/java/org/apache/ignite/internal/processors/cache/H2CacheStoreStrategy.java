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

package org.apache.ignite.internal.processors.cache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.cache.store.jdbc.CacheJdbcStoreSessionListener;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.h2.jdbcx.JdbcConnectionPool;
import org.h2.tools.RunScript;
import org.h2.tools.Server;

/**
 * {@link TestCacheStoreStrategy} backed by H2 in-memory database.
 */
public class H2CacheStoreStrategy implements TestCacheStoreStrategy {
    /** Pool to get {@link Connection}s from. */
    private final JdbcConnectionPool dataSrc;

    /** Script that creates CACHE table. */
    private static final String CREATE_CACHE_TABLE =
        "create table if not exists CACHE(k binary not null, v binary not null, PRIMARY KEY(k));";

    /** Script that creates STATS table. */
    private static final String CREATE_STATS_TABLES =
        "create table if not exists READS(id bigint auto_increment);\n" +
        "create table if not exists WRITES(id bigint auto_increment);\n" +
        "create table if not exists REMOVES(id bigint auto_increment);";

    /** Script that populates STATS table */
    private static final String POPULATE_STATS_TABLE =
        "delete from READS;\n" +
        "delete from WRITES;\n" +
        "delete from REMOVES;";


    /**
     * @throws IgniteCheckedException If failed.
     */
    public H2CacheStoreStrategy() throws IgniteCheckedException {
        try {
            Server.createTcpServer().start();
            dataSrc = H2CacheStoreSessionListenerFactory.createDataSource();

            try (Connection conn = connection()) {
                RunScript.execute(conn, new StringReader(CREATE_CACHE_TABLE));
                RunScript.execute(conn, new StringReader(CREATE_STATS_TABLES));
                RunScript.execute(conn, new StringReader(POPULATE_STATS_TABLE));
            }
        }
        catch (SQLException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int getReads() {
        return queryStats("reads");
    }

    /** {@inheritDoc} */
    @Override public int getWrites() {
        return queryStats("writes");
    }

    /** {@inheritDoc} */
    @Override public int getRemoves() {
        return queryStats("removes");
    }

    /**
     * @param tbl Table name.
     * @return Update statistics.
     */
    private int queryStats(String tbl) {
        return querySingleInt("select count(*) from " + tbl, "Failed to query store stats [table=" + tbl + "]");
    }

    /** {@inheritDoc} */
    @Override public int getStoreSize() {
        return querySingleInt("select count(*) from CACHE;", "Failed to query number of rows from CACHE table");
    }

    /** {@inheritDoc} */
    @Override public void resetStore() {
        try (Connection conn = connection()) {
            RunScript.execute(conn, new StringReader("delete from CACHE;"));
            RunScript.execute(conn, new StringReader(POPULATE_STATS_TABLE));
        }
        catch (SQLException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void putToStore(Object key, Object val) {
        Connection conn = null;
        try {
            conn = connection();
            H2CacheStore.putToDb(conn, key, val);
        }
        catch (SQLException e) {
            throw new IgniteException(e);
        }
        finally {
            U.closeQuiet(conn);
        }
    }

    /** {@inheritDoc} */
    @Override public void putAllToStore(Map<?, ?> data) {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = connection();
            stmt = conn.prepareStatement(H2CacheStore.MERGE);
            for (Map.Entry<?, ?> e : data.entrySet()) {
                stmt.setBinaryStream(1, new ByteArrayInputStream(H2CacheStore.serialize(e.getKey())));
                stmt.setBinaryStream(2, new ByteArrayInputStream(H2CacheStore.serialize(e.getValue())));
                stmt.addBatch();
            }
            stmt.executeBatch();
        }
        catch (SQLException e) {
            throw new IgniteException(e);
        }
        finally {
            U.closeQuiet(stmt);
            U.closeQuiet(conn);
        }
    }

    /** {@inheritDoc} */
    @Override public Object getFromStore(Object key) {
        Connection conn = null;
        try {
            conn = connection();
            return H2CacheStore.getFromDb(conn, key);
        }
        catch (SQLException e) {
            throw new IgniteException(e);
        }
        finally {
            U.closeQuiet(conn);
        }
    }

    /** {@inheritDoc} */
    @Override public void removeFromStore(Object key) {
        Connection conn = null;
        try {
            conn = connection();
            H2CacheStore.removeFromDb(conn, key);
        }
        catch (SQLException e) {
            throw new IgniteException(e);
        }
        finally {
            U.closeQuiet(conn);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isInStore(Object key) {
        return getFromStore(key) != null;
    }

    /**
     * @return New {@link Connection} from {@link #dataSrc}
     * @throws SQLException if failed
     */
    private Connection connection() throws SQLException {
        return dataSrc.getConnection();
    }

    /**
     * Retrieves single int value from {@link ResultSet} returned by given query.
     *
     * @param qry Query string (fully populated, with params).
     * @param errorMsg Message for {@link IgniteException} to bear in case of failure.
     * @return Requested value
     */
    private int querySingleInt(String qry, String errorMsg) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = connection();
            stmt = conn.prepareStatement(qry);
            rs = stmt.executeQuery();
            if (rs.next())
                return rs.getInt(1);
            else
                throw new IgniteException(errorMsg);
        }
        catch (SQLException e) {
            throw new IgniteException(e);
        }
        finally {
            U.closeQuiet(rs);
            U.closeQuiet(stmt);
            U.closeQuiet(conn);
        }
    }

    /** {@inheritDoc} */
    @Override public void updateCacheConfiguration(CacheConfiguration<Object, Object> cfg) {
        cfg.setCacheStoreSessionListenerFactories(new H2CacheStoreSessionListenerFactory());
    }

    /** {@inheritDoc} */
    @Override public Factory<? extends CacheStore<Object, Object>> getStoreFactory() {
        return new H2StoreFactory();
    }

    /** Serializable H2 backed cache store factory. */
    public static class H2StoreFactory implements Factory<CacheStore<Object, Object>> {
        /** {@inheritDoc} */
        @Override public CacheStore<Object, Object> create() {
            return new H2CacheStore();
        }
    }

    /** Serializable {@link Factory} producing H2 backed {@link CacheStoreSessionListener}s. */
    public static class H2CacheStoreSessionListenerFactory implements Factory<CacheStoreSessionListener> {
        /**
         * @return Connection pool
         */
        static JdbcConnectionPool createDataSource() {
            JdbcConnectionPool pool = JdbcConnectionPool.create("jdbc:h2:tcp://localhost/mem:TestDb;LOCK_MODE=0", "sa", "");
            pool.setMaxConnections(100);
            return pool;
        }

        /** {@inheritDoc} */
        @Override public CacheStoreSessionListener create() {
            CacheJdbcStoreSessionListener lsnr = new CacheJdbcStoreSessionListener();
            lsnr.setDataSource(createDataSource());
            return lsnr;
        }
    }

    /** H2 backed {@link CacheStoreAdapter} implementations */
    public static class H2CacheStore extends CacheStoreAdapter<Object, Object> {
        /** Store session */
        @CacheStoreSessionResource
        private CacheStoreSession ses;

        /** Template for an insert statement */
        private static final String MERGE = "merge into CACHE(k, v) values(?, ?);";

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Object, Object> clo, Object... args) {
            Connection conn = ses.attachment();
            assert conn != null;

            Statement stmt = null;
            ResultSet rs = null;
            try {
                stmt = conn.createStatement();
                rs = stmt.executeQuery("select * from CACHE");
                while (rs.next())
                    clo.apply(deserialize(rs.getBytes(1)), deserialize(rs.getBytes(2)));
            }
            catch (SQLException e) {
                throw new IgniteException(e);
            }
            finally {
                U.closeQuiet(rs);
                U.closeQuiet(stmt);
            }
        }

        /** {@inheritDoc} */
        @Override public Object load(Object key) throws CacheLoaderException {
            try {
                Connection conn = ses.attachment();
                Object res = getFromDb(conn, key);
                updateStats("reads");
                return res;
            }
            catch (SQLException e) {
                throw new CacheLoaderException("Failed to load object [key=" + key + ']', e);
            }
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> entry) throws CacheWriterException {
            try {
                Connection conn = ses.attachment();
                putToDb(conn, entry.getKey(), entry.getValue());
                updateStats("writes");
            }
            catch (SQLException e) {
                throw new CacheWriterException("Failed to write object [key=" + entry.getKey() + ", " +
                    "val=" + entry.getValue() + ']', e);
            }
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            try {
                Connection conn = ses.attachment();
                removeFromDb(conn, key);
                updateStats("removes");
            }
            catch (SQLException e) {
                throw new CacheWriterException("Failed to delete object [key=" + key + ']', e);
            }
        }

        /**
         * Selects from H2 and deserialize from bytes the value pointed by key.
         *
         * @param conn {@link Connection} to use.
         * @param key Key to look for.
         * @return Stored object or null if the key is missing from DB.
         * @throws SQLException If failed.
         */
        static Object getFromDb(Connection conn, Object key) throws SQLException {
            PreparedStatement stmt = null;
            ResultSet rs = null;
            try {
                stmt = conn.prepareStatement("select v from CACHE where k = ?");
                stmt.setBinaryStream(1, new ByteArrayInputStream(H2CacheStore.serialize(key)));
                rs = stmt.executeQuery();
                return rs.next() ? H2CacheStore.deserialize(rs.getBytes(1)) : null;
            }
            finally {
                U.closeQuiet(rs);
                U.closeQuiet(stmt);
            }
        }

        /**
         * Puts key-value pair to H2.
         *
         * @param conn {@link Connection} to use.
         * @param key Key.
         * @param val Value.
         * @throws SQLException If failed.
         */
        static void putToDb(Connection conn, Object key, Object val) throws SQLException {
            PreparedStatement stmt = null;
            try {
                stmt = conn.prepareStatement(H2CacheStore.MERGE);
                stmt.setBinaryStream(1, new ByteArrayInputStream(H2CacheStore.serialize(key)));
                stmt.setBinaryStream(2, new ByteArrayInputStream(H2CacheStore.serialize(val)));
                stmt.executeUpdate();
            }
            finally {
                U.closeQuiet(stmt);
            }
        }

        /**
         * Removes given key and its value from H2.
         *
         * @param conn {@link Connection} to invoke query upon.
         * @param key Key to remove.
         * @throws SQLException if failed.
         */
        static void removeFromDb(Connection conn, Object key) throws SQLException {
            PreparedStatement stmt = null;
            try {
                stmt = conn.prepareStatement("delete from CACHE where k = ?");
                stmt.setBinaryStream(1, new ByteArrayInputStream(H2CacheStore.serialize(key)));
                stmt.executeUpdate();
            }
            finally {
                U.closeQuiet(stmt);
            }
        }

        /**
         * Increments stored stats for given operation.
         *
         * @param tblName Table name
         */
        private void updateStats(String tblName) {
            Connection conn = ses.attachment();
            assert conn != null;
            Statement stmt = null;
            try {
                stmt = conn.createStatement();
                stmt.executeUpdate("insert into " + tblName + " default values");
            }
            catch (SQLException e) {
                throw new IgniteException("Failed to update H2 store usage stats", e);
            }
            finally {
                U.closeQuiet(stmt);
            }
        }

        /**
         * Turns given arbitrary object to byte array.
         *
         * @param obj Object to serialize
         * @return Bytes representation of given object.
         */
        static byte[] serialize(Object obj) {
            try (ByteArrayOutputStream b = new ByteArrayOutputStream()) {
                try (ObjectOutputStream o = new ObjectOutputStream(b)) {
                    o.writeObject(obj);
                }
                return b.toByteArray();
            }
            catch (Exception e) {
                throw new IgniteException("Failed to serialize object to byte array [obj=" + obj, e);
            }
        }

        /**
         * Deserializes an object from its byte array representation.
         *
         * @param bytes Byte array representation of the object.
         * @return Deserialized object.
         */
        public static Object deserialize(byte[] bytes) {
            try (ByteArrayInputStream b = new ByteArrayInputStream(bytes)) {
                try (ObjectInputStream o = new ObjectInputStream(b)) {
                    return o.readObject();
                }
            }
            catch (Exception e) {
                throw new IgniteException("Failed to deserialize object from byte array", e);
            }
        }
    }
}
