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

/*
 * Copyright 2021 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.index;

import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.client.Person;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.processors.query.h2.H2TableDescriptor;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Class for testing forced rebuilding of indexes.
 */
public class AffinityIndexShadowingTest extends AbstractRebuildIndexTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                cacheConfig(DEFAULT_CACHE_NAME),
                // Add one more cache to keep CacheGroup non-empty when the first cache will be destroyed during test.
                cacheConfig(DEFAULT_CACHE_NAME + 2)
            );
    }

    /** */
    private CacheConfiguration<Object, Object> cacheConfig(String cacheName) {
        return new CacheConfiguration<>(cacheName).setGroupName("GRP")
            .setIndexedTypes(PersonKey.class, Person.class)
            .setAffinity(new RendezvousAffinityFunction(false, 1));
    }

    /**
     * Checks that a new dynamic index shadows default affinity index correctly.
     * Expected that shadowed affinity index is consistent after user index was dropped.
     *
     * Scenario:
     * <li> Create cache. Key object must have dedicated affinity field.
     * <li> Populate caches with some data. Affinity index will holds the links to the data.
     * <li> Create new index 'IDX0' that will have AffinityKey as first column and wait all data be indexed.
     * <li> Restart the grid. Now, system affinity index will be shadowed by the user index 'IDX0'.
     * <li> Clear cache and populate once again.
     * TODO: actually, the system affinity index will not be involved here and contains broken links.
     * <li> Drop the index 'IDX0'.
     * <li> Restart the grid. The affinity index will be restored'.
     * <li> It is expected affinity index is wither ok or re-indexed.
     * TODO: actually, the system affinity index is broken and next put or index rebuild operation will fail.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityIndexShadowing() throws Exception {
        final String cacheName = DEFAULT_CACHE_NAME;
        final int cacheSize = 1_000;
        IgniteH2IndexingEx.prepareBeforeNodeStart();

        IgniteEx n = startGrid(0);
        populateCache(n.cache(cacheName), cacheSize);

        assertNotNull(index(n, n.cache(cacheName), H2TableDescriptor.AFFINITY_KEY_IDX_NAME));

        // Create user index that duplicates affinity-index.
        String idxName = "IDX0";
        createIdxAsync(n.cache(cacheName), idxName).get();

        assertNotNull(index(n, n.cache(cacheName), H2TableDescriptor.AFFINITY_KEY_IDX_NAME));
        assertNotNull(index(n, n.cache(cacheName), idxName));

        assertEquals(cacheSize, selectPersonByName(n.cache(cacheName)).size());

        // Restart.
        stopGrid(0);
        n = startGrid(0);

        // Invalidate old data. Expects, indices will be cleaned consistently.
        n.cache(cacheName).clear();
        populateCache(n.cache(cacheName), cacheSize);

        // Affinity index shadowed.
        assertNull(index(n, n.cache(cacheName), H2TableDescriptor.AFFINITY_KEY_IDX_NAME));
        assertNotNull(index(n, n.cache(cacheName), idxName));

        // Drop user index.
        dropIdx(n.cache(cacheName), idxName);

        assertNull(index(n, n.cache(cacheName), H2TableDescriptor.AFFINITY_KEY_IDX_NAME));
        assertNull(index(n, n.cache(cacheName), idxName));

        assertEquals(cacheSize, selectPersonByName(n.cache(cacheName)).size());

        // Restart.
        stopGrid(0);
        n = startGrid(0);

        IgniteInternalFuture<?> fut = indexRebuildFuture(n, CU.cacheId(cacheName));
        if (fut != null)
            fut.get(getTestTimeout()); // <-- Rebuild will fails due to broken Affinity index.

        assertNull(index(n, n.cache(cacheName), idxName));
        assertNotNull(index(n, n.cache(cacheName), H2TableDescriptor.AFFINITY_KEY_IDX_NAME));
        assertEquals(cacheSize, selectPersonByName(n.cache(cacheName)).size());
    }

    /**
     * Checks that a new dynamic index shadows default affinity index correctly.
     * The shadowed affinity index must be dropped together with the cache.
     *
     * Scenario:
     * <li> Create 2 caches in the same cache group. Key object must have dedicated affinity field.
     * (The second one is required to prevent whole group destroy when the first one will be dropped.)
     * <li> Populate caches with some data. Affinity index will holds the links to the data.
     * <li> Create new index 'IDX0' that will have AffinityKey as first column and wait all data be indexed.
     * <li> Restart the grid. Now, system affinity index will be shadowed by the user index 'IDX0'.
     * <li> Populate cache once again.
     * TODO: actually, the system affinity index will not be involved here and contains broken links.
     * <li> Drop the cache. 'IDX0' will be destroyed as well.
     * <li> Create cache with the same name and populate with the data.
     *  It is expected new cache will be operable.
     * TODO: actually, the system affinity index tree wasn't destroyed. Now it is resurrected and contains broken links.
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityIndexShadowing2() throws Exception {
        final String cacheName = DEFAULT_CACHE_NAME;
        final int cacheSize = 1_000;
        IgniteH2IndexingEx.prepareBeforeNodeStart();

        IgniteEx n = startGrid(0);
        populateCache(n.cache(cacheName), cacheSize);

        assertNotNull(index(n, n.cache(cacheName), H2TableDescriptor.AFFINITY_KEY_IDX_NAME));

        // Create user index that duplicates affinity-index.
        String idxName = "IDX0";
        createIdxAsync(n.cache(cacheName), idxName).get();

        assertNotNull(index(n, n.cache(cacheName), H2TableDescriptor.AFFINITY_KEY_IDX_NAME));
        assertNotNull(index(n, n.cache(cacheName), idxName));

        assertEquals(cacheSize, selectPersonByName(n.cache(cacheName)).size());

        // Restart.
        stopGrid(0);
        n = startGrid(0);

        assertEquals(cacheSize, selectPersonByName(n.cache(cacheName)).size());
        populateCache(n.cache(cacheName), cacheSize);

        // Affinity index shadowed.
        assertNull(index(n, n.cache(cacheName), H2TableDescriptor.AFFINITY_KEY_IDX_NAME));
        assertNotNull(index(n, n.cache(cacheName), idxName));

        // Recreate cache.
        n.destroyCache(cacheName);
        n.createCache(cacheConfig(cacheName));

        populateCache(n.cache(cacheName), cacheSize); // <-- Face corrupted affinity index here!

        assertNull(index(n, n.cache(cacheName), idxName));
        assertNotNull(index(n, n.cache(cacheName), H2TableDescriptor.AFFINITY_KEY_IDX_NAME));

        assertEquals(cacheSize, selectPersonByName(n.cache(cacheName)).size());
    }

    /**
     * Selection of all {@link Person} by name.
     * SQL: SELECT * FROM Person where name LIKE 'name_%';
     *
     * @param cache Cache.
     * @return List containing all query results.
     */
    private List<List<?>> selectPersonByName(IgniteCache<Integer, Person> cache) {
        return cache.query(new SqlFieldsQuery("SELECT * FROM Person where affId >= 0;")).getAll();
    }

    /**
     * Asynchronous creation of a new index for the cache of {@link Person}.
     * SQL: CREATE INDEX " + idxName + " ON Person(name)
     *
     * @param cache Cache.
     * @param idxName Index name.
     * @return Index creation future.
     */
    private IgniteInternalFuture<List<List<?>>> createIdxAsync(IgniteCache<Integer, Person> cache, String idxName) {
        return runAsync(() -> {
            String sql = "CREATE INDEX " + idxName + " ON Person(affId)";

            return cache.query(new SqlFieldsQuery(sql)).getAll();
        });
    }

    /**
     * Drop of an index for the cache of{@link Person}.
     * SQL: DROP INDEX " + idxName
     *
     * @param cache Cache.
     * @param idxName Index name.
     * @return Index creation future.
     */
    private List<List<?>> dropIdx(IgniteCache<Integer, Person> cache, String idxName) {
        return cache.query(new SqlFieldsQuery("DROP INDEX " + idxName)).getAll();
    }

    /** {@inheritDoc} */
    @Override protected @Nullable Index index(IgniteEx n, IgniteCache<Integer, Person> cache, String idxName) {
        try {
            return super.index(n, cache, idxName);
        }
        catch (Exception e) {
            return null;
        }
    }

    /**
     * Populate cache with {@link Person} sequentially.
     *
     * @param cache Cache.
     * @param cnt Entry count.
     */
    private void populateCache(IgniteCache<PersonKey, Person> cache, int cnt) {
        for (int i = 0; i < cnt; i++)
            cache.put(new PersonKey(i * 2, i), new Person(i, "name_" + i));
    }

    /**
     * Key class.
     */
    static class PersonKey {
        /** ID. */
        @QuerySqlField
        long keyId;

        /** Affinity key. */
        @AffinityKeyMapped
        @QuerySqlField
        long affId;

        PersonKey(long keyId, long affId) {
            this.keyId = keyId;
            this.affId = affId;
        }
    }
}
