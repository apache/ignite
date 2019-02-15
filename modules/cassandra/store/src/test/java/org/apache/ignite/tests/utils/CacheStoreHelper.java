/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.tests.utils;

import java.lang.reflect.Field;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.cache.store.cassandra.CassandraCacheStore;
import org.apache.ignite.cache.store.cassandra.datasource.DataSource;
import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;
import org.apache.ignite.logger.log4j.Log4JLogger;
import org.apache.log4j.Logger;
import org.springframework.core.io.Resource;

/**
 * Helper class utilized by unit tests to get appropriate instance of {@link CacheStore}
 */
public class CacheStoreHelper {
    /** */
    private static final Logger LOGGER = Logger.getLogger(CacheStoreHelper.class.getName());

    /** */
    public static CacheStore createCacheStore(String cacheName, Resource persistenceSettings, DataSource conn) {
        return createCacheStore(cacheName, persistenceSettings, conn, null, LOGGER);
    }

    /** */
    public static CacheStore createCacheStore(String cacheName, Resource persistenceSettings, DataSource conn,
        CacheStoreSession session) {
        return createCacheStore(cacheName, persistenceSettings, conn, session, LOGGER);
    }

    /** */
    public static CacheStore createCacheStore(String cacheName, Resource persistenceSettings, DataSource conn,
        Logger log) {
        return createCacheStore(cacheName, persistenceSettings, conn, null, log);
    }

    /** */
    public static CacheStore createCacheStore(String cacheName, Resource persistenceSettings, DataSource conn,
        CacheStoreSession session, Logger log) {
        CassandraCacheStore<Integer, Integer> cacheStore =
            new CassandraCacheStore<>(conn, new KeyValuePersistenceSettings(persistenceSettings),
                Runtime.getRuntime().availableProcessors());

        try {
            Field sesField = CassandraCacheStore.class.getDeclaredField("storeSes");
            Field logField = CassandraCacheStore.class.getDeclaredField("log");

            sesField.setAccessible(true);
            logField.setAccessible(true);

            sesField.set(cacheStore, session != null ? session : new TestCacheSession(cacheName));
            logField.set(cacheStore, new Log4JLogger(log));
        }
        catch (Throwable e) {
            throw new RuntimeException("Failed to initialize test Ignite cache store", e);
        }

        return cacheStore;
    }
}
