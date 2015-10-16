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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.cache.store.cassandra.utils.datasource.DataSource;
import org.apache.ignite.cache.store.cassandra.utils.persistence.KeyValuePersistenceSettings;
import org.apache.ignite.cache.store.cassandra.utils.persistence.PersistenceController;
import org.apache.ignite.cache.store.cassandra.utils.session.CassandraSession;
import org.apache.ignite.cache.store.cassandra.utils.session.GenericBatchExecutionAssistant;
import org.apache.ignite.cache.store.cassandra.utils.session.ExecutionAssistant;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.resources.CacheStoreSessionResource;
import org.apache.ignite.resources.LoggerResource;

/**
 * ${@link org.apache.ignite.cache.store.CacheStore} implementation to store Ignite cache key/value into Cassandra database
 * @param <K> Ignite cache key type
 * @param <V> Ignite cache value type
 */
public class CassandraCacheStore<K, V> implements CacheStore<K, V> {
    private static final String ATTR_CONN_PROP = "CASSANDRA_STORE_CONNECTION";

    /** Auto-injected store session. */
    @CacheStoreSessionResource
    private CacheStoreSession storeSession;

    /** Auto-injected logger instance. */
    @LoggerResource
    private IgniteLogger logger;

    private DataSource dataSource;
    private PersistenceController controller;

    public CassandraCacheStore(DataSource dataSource, KeyValuePersistenceSettings settings) {
        this.dataSource = dataSource;
        this.controller = new PersistenceController(settings);
    }

    @Override public void loadCache(IgniteBiInClosure<K, V> closure, Object... args) throws CacheLoaderException {
    }

    @Override public void sessionEnd(boolean commit) throws CacheWriterException {
        if (storeSession == null || storeSession.transaction() == null)
            return;

        CassandraSession cassandraSession = (CassandraSession)storeSession.properties().remove(ATTR_CONN_PROP);
        if (cassandraSession != null) {
            try {
                cassandraSession.close();
            }
            catch (Throwable ignored) {
            }
        }
    }

    @SuppressWarnings({"unchecked"})
    @Override public V load(final K key) throws CacheLoaderException {
        if (key == null)
            return null;

        CassandraSession ses = getCassandraSession();

        try {
            return ses.execute(new ExecutionAssistant<V>() {
                @Override public boolean tableExistenceRequired() {
                    return false;
                }

                @Override public String getStatement() {
                    return controller.getLoadStatement(false);
                }

                @Override public BoundStatement bindStatement(PreparedStatement statement) {
                    return controller.bindKey(statement, key);
                }

                @Override public KeyValuePersistenceSettings getPersistenceSettings() {
                    return controller.getPersistenceSettings();
                }

                @Override public String operationName() {
                    return "READ";
                }

                @Override public V process(Row row) {
                    return row == null ? null : (V)controller.buildValueObject(row);
                }
            });
        }
        finally {
            closeCassandraSession(ses);
        }
    }

    @SuppressWarnings("unchecked")
    @Override public Map<K, V> loadAll(Iterable<? extends K> keys) throws CacheLoaderException {
        if (keys == null || !keys.iterator().hasNext())
            return new HashMap<>();

        CassandraSession ses = getCassandraSession();

        try {
            return ses.execute(new GenericBatchExecutionAssistant<Map<K, V>, K>() {
                private Map<K, V> data = new HashMap<>();

                @Override public String getStatement() {
                    return controller.getLoadStatement(true);
                }

                @Override  public BoundStatement bindStatement(PreparedStatement statement, K key) {
                    return controller.bindKey(statement, key);
                }

                @Override public KeyValuePersistenceSettings getPersistenceSettings() {
                    return controller.getPersistenceSettings();
                }

                @Override public String operationName() {
                    return "BULK_READ";
                }

                @Override public Map<K, V> processedData() {
                    return data;
                }

                @Override protected void process(Row row) {
                    data.put((K)controller.buildKeyObject(row), (V)controller.buildValueObject(row));
                }
            }, keys);
        }
        finally {
            closeCassandraSession(ses);
        }
    }

    @Override public void write(final Cache.Entry<? extends K, ? extends V> entry) throws CacheWriterException {
        if (entry == null || entry.getKey() == null)
            return;

        CassandraSession ses = getCassandraSession();

        try {
            ses.execute(new ExecutionAssistant<Void>() {
                @Override public boolean tableExistenceRequired() {
                    return true;
                }

                @Override public String getStatement() {
                    return controller.getWriteStatement();
                }

                @Override public BoundStatement bindStatement(PreparedStatement statement) {
                    return controller.bindKeyValue(statement, entry.getKey(), entry.getValue());
                }

                @Override public KeyValuePersistenceSettings getPersistenceSettings() {
                    return controller.getPersistenceSettings();
                }

                @Override public String operationName() {
                    return "WRITE";
                }

                @Override public Void process(Row row) {
                    return null;
                }
            });
        }
        finally {
            closeCassandraSession(ses);
        }
    }

    @Override public void writeAll(Collection<Cache.Entry<? extends K, ? extends V>> entries) throws CacheWriterException {
        if (entries == null || entries.isEmpty())
            return;

        CassandraSession ses = getCassandraSession();

        try {
            ses.execute(new GenericBatchExecutionAssistant<Void, Cache.Entry<? extends K, ? extends V>>() {
                @Override public String getStatement() {
                    return controller.getWriteStatement();
                }

                @Override public BoundStatement bindStatement(PreparedStatement statement,
                    Cache.Entry<? extends K, ? extends V> entry) {
                    return controller.bindKeyValue(statement, entry.getKey(), entry.getValue());
                }

                @Override public KeyValuePersistenceSettings getPersistenceSettings() {
                    return controller.getPersistenceSettings();
                }

                @Override public String operationName() {
                    return "BULK_WRITE";
                }

                @Override public boolean tableExistenceRequired() {
                    return true;
                }
            }, entries);
        }
        finally {
            closeCassandraSession(ses);
        }
    }

    @Override public void delete(final Object key) throws CacheWriterException {
        if (key == null)
            return;

        CassandraSession ses = getCassandraSession();

        try {
            ses.execute(new ExecutionAssistant<Void>() {
                @Override public boolean tableExistenceRequired() {
                    return false;
                }

                @Override public String getStatement() {
                    return controller.getDeleteStatement();
                }

                @Override public BoundStatement bindStatement(PreparedStatement statement) {
                    return controller.bindKey(statement, key);
                }


                @Override public KeyValuePersistenceSettings getPersistenceSettings() {
                    return controller.getPersistenceSettings();
                }

                @Override public String operationName() {
                    return "DELETE";
                }

                @Override public Void process(Row row) {
                    return null;
                }
            });
        }
        finally {
            closeCassandraSession(ses);
        }
    }

    @Override public void deleteAll(Collection<?> keys) throws CacheWriterException {
        if (keys == null || keys.isEmpty())
            return;

        CassandraSession ses = getCassandraSession();

        try {
            ses.execute(new GenericBatchExecutionAssistant<Void, Object>() {
                @Override public String getStatement() {
                    return controller.getDeleteStatement();
                }

                @Override public BoundStatement bindStatement(PreparedStatement statement, Object key) {
                    return controller.bindKey(statement, key);
                }

                @Override public KeyValuePersistenceSettings getPersistenceSettings() {
                    return controller.getPersistenceSettings();
                }

                @Override public String operationName() {
                    return "BULK_DELETE";
                }
            }, keys);
        }
        finally {
            closeCassandraSession(ses);
        }
    }

    private CassandraSession getCassandraSession() {
        if (storeSession == null || storeSession.transaction() == null)
            return dataSource.session(logger != null ? logger : new NullLogger());

        CassandraSession ses = (CassandraSession)storeSession.properties().get(ATTR_CONN_PROP);

        if (ses == null) {
            ses = dataSource.session(logger != null ? logger : new NullLogger());
            storeSession.properties().put(ATTR_CONN_PROP, ses);
        }

        return ses;
    }

    private void closeCassandraSession(CassandraSession ses) {
        if (ses != null && (storeSession == null || storeSession.transaction() == null)) {
            try {
                ses.close();
            }
            catch (Throwable ignored) {
            }
        }
    }
}
