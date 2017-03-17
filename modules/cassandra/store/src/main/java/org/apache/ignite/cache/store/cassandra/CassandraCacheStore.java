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

package org.apache.ignite.cache.store.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.cache.store.cassandra.datasource.DataSource;
import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;
import org.apache.ignite.cache.store.cassandra.persistence.PersistenceController;
import org.apache.ignite.cache.store.cassandra.session.CassandraSession;
import org.apache.ignite.cache.store.cassandra.session.ExecutionAssistant;
import org.apache.ignite.cache.store.cassandra.session.GenericBatchExecutionAssistant;
import org.apache.ignite.cache.store.cassandra.session.LoadCacheCustomQueryWorker;
import org.apache.ignite.cache.store.cassandra.session.transaction.DeleteMutation;
import org.apache.ignite.cache.store.cassandra.session.transaction.Mutation;
import org.apache.ignite.cache.store.cassandra.session.transaction.WriteMutation;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.apache.ignite.resources.LoggerResource;

/**
 * Implementation of {@link CacheStore} backed by Cassandra database.
 *
 * @param <K> Ignite cache key type.
 * @param <V> Ignite cache value type.
 */
public class CassandraCacheStore<K, V> implements CacheStore<K, V> {
    /** Buffer to store mutations performed withing transaction. */
    private static final String TRANSACTION_BUFFER = "CASSANDRA_TRANSACTION_BUFFER";

    /** Auto-injected store session. */
    @SuppressWarnings("unused")
    @CacheStoreSessionResource
    private CacheStoreSession storeSes;

    /** Auto-injected logger instance. */
    @SuppressWarnings("unused")
    @LoggerResource
    private IgniteLogger log;

    /** Cassandra data source. */
    private DataSource dataSrc;

    /** Max workers thread count. These threads are responsible for load cache. */
    private int maxPoolSize = Runtime.getRuntime().availableProcessors();

    /** Controller component responsible for serialization logic. */
    private final PersistenceController controller;

    /**
     * Store constructor.
     *
     * @param dataSrc Data source.
     * @param settings Persistence settings for Ignite key and value objects.
     * @param maxPoolSize Max workers thread count.
     */
    public CassandraCacheStore(DataSource dataSrc, KeyValuePersistenceSettings settings, int maxPoolSize) {
        this.dataSrc = dataSrc;
        this.controller = new PersistenceController(settings);
        this.maxPoolSize = maxPoolSize;
    }

    /** {@inheritDoc} */
    @Override public void loadCache(IgniteBiInClosure<K, V> clo, Object... args) throws CacheLoaderException {
        if (clo == null)
            return;

        if (args == null || args.length == 0)
            args = new String[] {"select * from " + controller.getPersistenceSettings().getKeyspace() + "." + cassandraTable() + ";"};

        ExecutorService pool = null;

        Collection<Future<?>> futs = new ArrayList<>(args.length);

        try {
            pool = Executors.newFixedThreadPool(maxPoolSize);

            CassandraSession ses = getCassandraSession();

            for (Object obj : args) {
                if (obj == null || !(obj instanceof String) || !((String)obj).trim().toLowerCase().startsWith("select"))
                    continue;

                futs.add(pool.submit(new LoadCacheCustomQueryWorker<>(ses, (String) obj, controller, log, clo)));
            }

            for (Future<?> fut : futs)
                U.get(fut);

            if (log != null && log.isDebugEnabled() && storeSes != null)
                log.debug("Cache loaded from db: " + storeSes.cacheName());
        }
        catch (IgniteCheckedException e) {
            if (storeSes != null)
                throw new CacheLoaderException("Failed to load Ignite cache: " + storeSes.cacheName(), e.getCause());
            else
                throw new CacheLoaderException("Failed to load cache", e.getCause());
        }
        finally {
            U.shutdownNow(getClass(), pool, log);
        }
    }

    /** {@inheritDoc} */
    @Override public void sessionEnd(boolean commit) throws CacheWriterException {
        if (!storeSes.isWithinTransaction())
            return;

        List<Mutation> mutations = mutations();
        if (mutations == null || mutations.isEmpty())
            return;

        CassandraSession ses = getCassandraSession();

        try {
            ses.execute(mutations);
        }
        finally {
            mutations.clear();
            U.closeQuiet(ses);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public V load(final K key) throws CacheLoaderException {
        if (key == null)
            return null;

        CassandraSession ses = getCassandraSession();

        try {
            return ses.execute(new ExecutionAssistant<V>() {
                /** {@inheritDoc} */
                @Override public boolean tableExistenceRequired() {
                    return false;
                }

                /** {@inheritDoc} */
                @Override public String getTable() {
                    return cassandraTable();
                }

                /** {@inheritDoc} */
                @Override public String getStatement() {
                    return controller.getLoadStatement(cassandraTable(), false);
                }

                /** {@inheritDoc} */
                @Override public BoundStatement bindStatement(PreparedStatement statement) {
                    return controller.bindKey(statement, key);
                }

                /** {@inheritDoc} */
                @Override public KeyValuePersistenceSettings getPersistenceSettings() {
                    return controller.getPersistenceSettings();
                }

                /** {@inheritDoc} */
                @Override public String operationName() {
                    return "READ";
                }

                /** {@inheritDoc} */
                @Override public V process(Row row) {
                    return row == null ? null : (V)controller.buildValueObject(row);
                }
            });
        }
        finally {
            U.closeQuiet(ses);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Map<K, V> loadAll(Iterable<? extends K> keys) throws CacheLoaderException {
        if (keys == null || !keys.iterator().hasNext())
            return new HashMap<>();

        CassandraSession ses = getCassandraSession();

        try {
            return ses.execute(new GenericBatchExecutionAssistant<Map<K, V>, K>() {
                private Map<K, V> data = new HashMap<>();

                /** {@inheritDoc} */
                @Override public String getTable() {
                    return cassandraTable();
                }

                /** {@inheritDoc} */
                @Override public String getStatement() {
                    return controller.getLoadStatement(cassandraTable(), true);
                }

                /** {@inheritDoc} */
                @Override  public BoundStatement bindStatement(PreparedStatement statement, K key) {
                    return controller.bindKey(statement, key);
                }

                /** {@inheritDoc} */
                @Override public KeyValuePersistenceSettings getPersistenceSettings() {
                    return controller.getPersistenceSettings();
                }

                /** {@inheritDoc} */
                @Override public String operationName() {
                    return "BULK_READ";
                }

                /** {@inheritDoc} */
                @Override public Map<K, V> processedData() {
                    return data;
                }

                /** {@inheritDoc} */
                @Override protected void process(Row row) {
                    data.put((K)controller.buildKeyObject(row), (V)controller.buildValueObject(row));
                }
            }, keys);
        }
        finally {
            U.closeQuiet(ses);
        }
    }

    /** {@inheritDoc} */
    @Override public void write(final Cache.Entry<? extends K, ? extends V> entry) throws CacheWriterException {
        if (entry == null || entry.getKey() == null)
            return;

        if (storeSes.isWithinTransaction()) {
            accumulate(new WriteMutation(entry, cassandraTable(), controller));
            return;
        }

        CassandraSession ses = getCassandraSession();

        try {
            ses.execute(new ExecutionAssistant<Void>() {
                /** {@inheritDoc} */
                @Override public boolean tableExistenceRequired() {
                    return true;
                }

                /** {@inheritDoc} */
                @Override public String getTable() {
                    return cassandraTable();
                }

                /** {@inheritDoc} */
                @Override public String getStatement() {
                    return controller.getWriteStatement(cassandraTable());
                }

                /** {@inheritDoc} */
                @Override public BoundStatement bindStatement(PreparedStatement statement) {
                    return controller.bindKeyValue(statement, entry.getKey(), entry.getValue());
                }

                /** {@inheritDoc} */
                @Override public KeyValuePersistenceSettings getPersistenceSettings() {
                    return controller.getPersistenceSettings();
                }

                /** {@inheritDoc} */
                @Override public String operationName() {
                    return "WRITE";
                }

                /** {@inheritDoc} */
                @Override public Void process(Row row) {
                    return null;
                }
            });
        }
        finally {
            U.closeQuiet(ses);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeAll(Collection<Cache.Entry<? extends K, ? extends V>> entries) throws CacheWriterException {
        if (entries == null || entries.isEmpty())
            return;

        if (storeSes.isWithinTransaction()) {
            for (Cache.Entry<?, ?> entry : entries)
                accumulate(new WriteMutation(entry, cassandraTable(), controller));

            return;
        }

        CassandraSession ses = getCassandraSession();

        try {
            ses.execute(new GenericBatchExecutionAssistant<Void, Cache.Entry<? extends K, ? extends V>>() {
                /** {@inheritDoc} */
                @Override public String getTable() {
                    return cassandraTable();
                }

                /** {@inheritDoc} */
                @Override public String getStatement() {
                    return controller.getWriteStatement(cassandraTable());
                }

                /** {@inheritDoc} */
                @Override public BoundStatement bindStatement(PreparedStatement statement,
                    Cache.Entry<? extends K, ? extends V> entry) {
                    return controller.bindKeyValue(statement, entry.getKey(), entry.getValue());
                }

                /** {@inheritDoc} */
                @Override public KeyValuePersistenceSettings getPersistenceSettings() {
                    return controller.getPersistenceSettings();
                }

                /** {@inheritDoc} */
                @Override public String operationName() {
                    return "BULK_WRITE";
                }

                /** {@inheritDoc} */
                @Override public boolean tableExistenceRequired() {
                    return true;
                }
            }, entries);
        }
        finally {
            U.closeQuiet(ses);
        }
    }

    /** {@inheritDoc} */
    @Override public void delete(final Object key) throws CacheWriterException {
        if (key == null)
            return;

        if (storeSes.isWithinTransaction()) {
            accumulate(new DeleteMutation(key, cassandraTable(), controller));
            return;
        }

        CassandraSession ses = getCassandraSession();

        try {
            ses.execute(new ExecutionAssistant<Void>() {
                /** {@inheritDoc} */
                @Override public boolean tableExistenceRequired() {
                    return false;
                }

                /** {@inheritDoc} */
                @Override public String getTable() {
                    return cassandraTable();
                }

                /** {@inheritDoc} */
                @Override public String getStatement() {
                    return controller.getDeleteStatement(cassandraTable());
                }

                /** {@inheritDoc} */
                @Override public BoundStatement bindStatement(PreparedStatement statement) {
                    return controller.bindKey(statement, key);
                }


                /** {@inheritDoc} */
                @Override public KeyValuePersistenceSettings getPersistenceSettings() {
                    return controller.getPersistenceSettings();
                }

                /** {@inheritDoc} */
                @Override public String operationName() {
                    return "DELETE";
                }

                /** {@inheritDoc} */
                @Override public Void process(Row row) {
                    return null;
                }
            });
        }
        finally {
            U.closeQuiet(ses);
        }
    }

    /** {@inheritDoc} */
    @Override public void deleteAll(Collection<?> keys) throws CacheWriterException {
        if (keys == null || keys.isEmpty())
            return;

        if (storeSes.isWithinTransaction()) {
            for (Object key : keys)
                accumulate(new DeleteMutation(key, cassandraTable(), controller));

            return;
        }

        CassandraSession ses = getCassandraSession();

        try {
            ses.execute(new GenericBatchExecutionAssistant<Void, Object>() {
                /** {@inheritDoc} */
                @Override public String getTable() {
                    return cassandraTable();
                }

                /** {@inheritDoc} */
                @Override public String getStatement() {
                    return controller.getDeleteStatement(cassandraTable());
                }

                /** {@inheritDoc} */
                @Override public BoundStatement bindStatement(PreparedStatement statement, Object key) {
                    return controller.bindKey(statement, key);
                }

                /** {@inheritDoc} */
                @Override public KeyValuePersistenceSettings getPersistenceSettings() {
                    return controller.getPersistenceSettings();
                }

                /** {@inheritDoc} */
                @Override public String operationName() {
                    return "BULK_DELETE";
                }
            }, keys);
        }
        finally {
            U.closeQuiet(ses);
        }
    }

    /**
     * Gets Cassandra session wrapper or creates new if it doesn't exist.
     * This wrapper hides all the low-level Cassandra interaction details by providing only high-level methods.
     *
     * @return Cassandra session wrapper.
     */
    private CassandraSession getCassandraSession() {
        return dataSrc.session(log != null ? log : new NullLogger());
    }

    /**
     * Returns table name to use for all Cassandra based operations (READ/WRITE/DELETE).
     *
     * @return Table name.
     */
    private String cassandraTable() {
        return controller.getPersistenceSettings().getTable() != null ?
            controller.getPersistenceSettings().getTable() : storeSes.cacheName().trim().toLowerCase();
    }

    /**
     * Accumulates mutation in the transaction buffer.
     *
     * @param mutation Mutation operation.
     */
    private void accumulate(Mutation mutation) {
        //noinspection unchecked
        List<Mutation> mutations = (List<Mutation>)storeSes.properties().get(TRANSACTION_BUFFER);

        if (mutations == null) {
            mutations = new LinkedList<>();
            storeSes.properties().put(TRANSACTION_BUFFER, mutations);
        }

        mutations.add(mutation);
    }

    /**
     * Returns all the mutations performed withing transaction.
     *
     * @return Mutations
     */
    private List<Mutation> mutations() {
        //noinspection unchecked
        return (List<Mutation>)storeSes.properties().get(TRANSACTION_BUFFER);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CassandraCacheStore.class, this);
    }
}
