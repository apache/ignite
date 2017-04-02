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

package org.apache.ignite.internal.processors.cache.index;

import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.index.SchemaOperationException;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Tests for dynamic index creation.
 */
@SuppressWarnings({"unchecked", "ThrowableResultOfMethodCallIgnored"})
public class DynamicIndexSelfTest extends AbstractSchemaSelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        grid(0).getOrCreateCache(cacheConfiguration());
        grid(0).getOrCreateCache(caseSensitiveCacheConfiguration());
        grid(0).getOrCreateCache(aliasCacheConfiguration());

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME);
        assertNoIndex(CACHE_NAME_SENSITIVE, TBL_NAME, IDX_NAME);
        assertNoIndex(CACHE_NAME_ALIAS, TBL_NAME_2, IDX_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).destroyCache(CACHE_NAME);
        grid(0).destroyCache(CACHE_NAME_SENSITIVE);
        grid(0).destroyCache(CACHE_NAME_ALIAS);

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * Test simple index create.
     *
     * @throws Exception If failed.
     */
    public void testCreate() throws Exception {
        final QueryIndex idx = index(IDX_NAME, field(FIELD_NAME));

        queryProcessor(grid(0)).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false).get();
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME, field(FIELD_NAME));

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                queryProcessor(grid(0)).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false).get();

                return null;
            }
        }, SchemaOperationException.class, null);

        queryProcessor(grid(0)).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, true).get();
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME, field(FIELD_NAME));
    }

    /**
     * Test simple index create with schema case sensitivity considered.
     *
     * @throws Exception If failed.
     */
    public void testCreateCaseSensitive() throws Exception {
        QueryIndex idx = index(IDX_NAME, field("Id"), field(FIELD_NAME), field("id", true));

        queryProcessor(grid(0)).dynamicIndexCreate(CACHE_NAME_SENSITIVE, TBL_NAME, idx, false).get();
        assertIndex(CACHE_NAME_SENSITIVE, TBL_NAME, IDX_NAME, field("Id"), field(FIELD_NAME), field("id", true));

        queryProcessor(grid(0)).dynamicIndexCreate(CACHE_NAME_SENSITIVE, TBL_NAME, idx, true).get();
        assertIndex(CACHE_NAME_SENSITIVE, TBL_NAME, IDX_NAME, field("Id"), field(FIELD_NAME), field("id", true));
    }

    /**
     * Test simple index create with field alias in effect.
     *
     * @throws Exception If failed.
     */
    public void testCreateWithAlias() throws Exception {
        QueryIndex idx = index(IDX_NAME, field(FIELD_NAME), field("id", true));

        queryProcessor(grid(0)).dynamicIndexCreate(CACHE_NAME_ALIAS, TBL_NAME_2, idx, false).get();
        assertIndex(CACHE_NAME_ALIAS, TBL_NAME_2, IDX_NAME, field(FIELD_NAME), field("id", true));

        queryProcessor(grid(0)).dynamicIndexCreate(CACHE_NAME_ALIAS, TBL_NAME_2, idx, true).get();
        assertIndex(CACHE_NAME_ALIAS, TBL_NAME_2, IDX_NAME, field(FIELD_NAME), field("id", true));
    }

    /**
     * Test simple index drop.
     *
     * @throws Exception If failed.
     */
    public void testDrop() throws Exception {
        QueryIndex idx = index(IDX_NAME, field(FIELD_NAME));

        queryProcessor(grid(0)).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false).get();
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME, field(FIELD_NAME));

        queryProcessor(grid(0)).dynamicIndexDrop(CACHE_NAME, IDX_NAME, false).get();
        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                queryProcessor(grid(0)).dynamicIndexDrop(CACHE_NAME, IDX_NAME, false).get();

                return null;
            }
        }, SchemaOperationException.class, null);

        queryProcessor(grid(0)).dynamicIndexDrop(CACHE_NAME, IDX_NAME, true).get();
        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME);
    }

    /**
     * @return Default cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        return new CacheConfiguration<KeyClass, ValueClass>()
            .setName(CACHE_NAME)
            .setIndexedTypes(KeyClass.class, ValueClass.class);
    }

    /**
     * @return Default cache configuration.
     */
    private CacheConfiguration caseSensitiveCacheConfiguration() {
        return new CacheConfiguration<KeyClass2, ValueClass>()
            .setName(CACHE_NAME_SENSITIVE)
            .setSqlEscapeAll(true)
            .setIndexedTypes(KeyClass2.class, ValueClass.class);
    }

    /**
     * @return Default cache configuration.
     */
    private CacheConfiguration aliasCacheConfiguration() {
        return new CacheConfiguration<KeyClass, ValueClass2>()
            .setName(CACHE_NAME_ALIAS)
            .setIndexedTypes(KeyClass.class, ValueClass2.class);
    }
}
