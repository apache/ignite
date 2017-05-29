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

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;

import javax.cache.CacheException;
import java.util.Arrays;
import java.util.List;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 * Tests for dynamic index creation.
 */
@SuppressWarnings({"unchecked", "ThrowableResultOfMethodCallIgnored"})
public abstract class DynamicIndexAbstractBasicSelfTest extends DynamicIndexAbstractSelfTest {
    /** Node index for regular server (coordinator). */
    protected static final int IDX_SRV_CRD = 0;

    /** Node index for regular server (not coordinator). */
    protected static final int IDX_SRV_NON_CRD = 1;

    /** Node index for regular client. */
    protected static final int IDX_CLI = 2;

    /** Node index for server which doesn't pass node filter. */
    protected static final int IDX_SRV_FILTERED = 3;

    /** Node index for client with near-only cache. */
    protected static final int IDX_CLI_NEAR_ONLY = 4;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        for (IgniteConfiguration cfg : configurations())
            Ignition.start(cfg);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        node().destroyCache(CACHE_NAME);

        super.afterTest();
    }

    /**
     * Initialize cache for tests.
     *
     * @param mode Mode.
     * @param atomicityMode Atomicity mode.
     * @param near Near flag.
     */
    private void initialize(CacheMode mode, CacheAtomicityMode atomicityMode, boolean near) {
        node().getOrCreateCache(cacheConfiguration(mode, atomicityMode, near));

        grid(IDX_CLI_NEAR_ONLY).getOrCreateNearCache(CACHE_NAME, new NearCacheConfiguration<>());

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);

        loadInitialData();
    }

    /**
     * Create cache with the given cache mode and atomicity mode.
     *
     * @param mode Mode.
     * @param atomicityMode Atomicity mode.
     * @param near Whether near cache should be initialized.
     * @return Cache configuration.
     */
    private CacheConfiguration<KeyClass, ValueClass> cacheConfiguration(CacheMode mode,
        CacheAtomicityMode atomicityMode, boolean near) {
        CacheConfiguration<KeyClass, ValueClass> ccfg = cacheConfiguration();

        ccfg.setCacheMode(mode);
        ccfg.setAtomicityMode(atomicityMode);

        if (near)
            ccfg.setNearConfiguration(new NearCacheConfiguration<KeyClass, ValueClass>());

        return ccfg;
    }

    /**
     * Load initial data.
     */
    private void loadInitialData() {
        put(node(), 0, KEY_BEFORE);
    }

    /**
     * Test simple index create for PARTITIONED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    public void testCreatePartitionedAtomic() throws Exception {
        checkCreate(PARTITIONED, ATOMIC, false);
    }

    /**
     * Test simple index create for PARTITIONED ATOMIC cache with near cache.
     *
     * @throws Exception If failed.
     */
    public void testCreatePartitionedAtomicNear() throws Exception {
        checkCreate(PARTITIONED, ATOMIC, true);
    }

    /**
     * Test simple index create for PARTITIONED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    public void testCreatePartitionedTransactional() throws Exception {
        checkCreate(PARTITIONED, TRANSACTIONAL, false);
    }

    /**
     * Test simple index create for PARTITIONED TRANSACTIONAL cache with near cache.
     *
     * @throws Exception If failed.
     */
    public void testCreatePartitionedTransactionalNear() throws Exception {
        checkCreate(PARTITIONED, TRANSACTIONAL, true);
    }

    /**
     * Test simple index create for REPLICATED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateReplicatedAtomic() throws Exception {
        checkCreate(REPLICATED, ATOMIC, false);
    }

    /**
     * Test simple index create for REPLICATED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateReplicatedTransactional() throws Exception {
        checkCreate(REPLICATED, TRANSACTIONAL, false);
    }

    /**
     * Check normal create operation.
     *
     * @param mode Mode.
     * @param atomicityMode Atomicity mode.
     * @param near Near flag.
     * @throws Exception If failed.
     */
    private void checkCreate(CacheMode mode, CacheAtomicityMode atomicityMode, boolean near) throws Exception {
        initialize(mode, atomicityMode, near);

        final QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1_ESCAPED));

        dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false);
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, field(FIELD_NAME_1_ESCAPED));

        assertSchemaException(new RunnableX() {
            @Override public void run() throws Exception {
                dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false);
            }
        }, IgniteQueryErrorCode.INDEX_ALREADY_EXISTS);

        dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, true);
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, field(FIELD_NAME_1_ESCAPED));

        assertSimpleIndexOperations(SQL_SIMPLE_FIELD_1);

        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_ARG_1);
    }

    /**
     * Test composite index creation for PARTITIONED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateCompositePartitionedAtomic() throws Exception {
        checkCreateComposite(PARTITIONED, ATOMIC, false);
    }

    /**
     * Test composite index creation for PARTITIONED ATOMIC cache with near cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateCompositePartitionedAtomicNear() throws Exception {
        checkCreateComposite(PARTITIONED, ATOMIC, true);
    }

    /**
     * Test composite index creation for PARTITIONED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateCompositePartitionedTransactional() throws Exception {
        checkCreateComposite(PARTITIONED, TRANSACTIONAL, false);
    }

    /**
     * Test composite index creation for PARTITIONED TRANSACTIONAL cache with near cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateCompositePartitionedTransactionalNear() throws Exception {
        checkCreateComposite(PARTITIONED, TRANSACTIONAL, true);
    }

    /**
     * Test composite index creation for REPLICATED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateCompositeReplicatedAtomic() throws Exception {
        checkCreateComposite(REPLICATED, ATOMIC, false);
    }

    /**
     * Test composite index creation for REPLICATED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateCompositeReplicatedTransactional() throws Exception {
        checkCreateComposite(REPLICATED, TRANSACTIONAL, false);
    }

    /**
     * Check composite index creation.
     *
     * @param mode Mode.
     * @param atomicityMode Atomicity mode.
     * @param near Near flag.
     * @throws Exception If failed.
     */
    private void checkCreateComposite(CacheMode mode, CacheAtomicityMode atomicityMode, boolean near) throws Exception {
        initialize(mode, atomicityMode, near);

        final QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1_ESCAPED), field(alias(FIELD_NAME_2_ESCAPED)));

        dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false);
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, field(FIELD_NAME_1_ESCAPED), field(alias(FIELD_NAME_2_ESCAPED)));

        assertCompositeIndexOperations(SQL_COMPOSITE);

        assertIndexUsed(IDX_NAME_1, SQL_COMPOSITE, SQL_ARG_1, SQL_ARG_2);
    }

    /**
     * Test create when cache doesn't exist for PARTITIONED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoCachePartitionedAtomic() throws Exception {
        checkCreateNotCache(PARTITIONED, ATOMIC, false);
    }

    /**
     * Test create when cache doesn't exist for PARTITIONED ATOMIC cache with near cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoCachePartitionedAtomicNear() throws Exception {
        checkCreateNotCache(PARTITIONED, ATOMIC, true);
    }

    /**
     * Test create when cache doesn't exist for PARTITIONED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoCachePartitionedTransactional() throws Exception {
        checkCreateNotCache(PARTITIONED, TRANSACTIONAL, false);
    }

    /**
     * Test create when cache doesn't exist for PARTITIONED TRANSACTIONAL cache with near cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoCachePartitionedTransactionalNear() throws Exception {
        checkCreateNotCache(PARTITIONED, TRANSACTIONAL, true);
    }

    /**
     * Test create when cache doesn't exist for REPLICATED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoCacheReplicatedAtomic() throws Exception {
        checkCreateNotCache(REPLICATED, ATOMIC, false);
    }

    /**
     * Test create when cache doesn't exist for REPLICATED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoCacheReplicatedTransactional() throws Exception {
        checkCreateNotCache(REPLICATED, TRANSACTIONAL, false);
    }

    /**
     * Check create when cache doesn't exist.
     *
     * @param mode Mode.
     * @param atomicityMode Atomicity mode.
     * @param near Near flag.
     * @throws Exception If failed.
     */
    private void checkCreateNotCache(CacheMode mode, CacheAtomicityMode atomicityMode, boolean near) throws Exception {
        initialize(mode, atomicityMode, near);

        final QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1_ESCAPED));

        try {
            String cacheName = randomString();

            queryProcessor(node()).dynamicIndexCreate(cacheName, cacheName, TBL_NAME, idx, false).get();
        }
        catch (SchemaOperationException e) {
            assertEquals(SchemaOperationException.CODE_CACHE_NOT_FOUND, e.code());

            assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);

            return;
        }
        catch (Exception e) {
            fail("Unexpected exception: " + e);
        }

        fail(SchemaOperationException.class.getSimpleName() +  " is not thrown.");
    }

    /**
     * Test create when table doesn't exist for PARTITIONED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoTablePartitionedAtomic() throws Exception {
        checkCreateNoTable(PARTITIONED, ATOMIC, false);
    }

    /**
     * Test create when table doesn't exist for PARTITIONED ATOMIC cache with near cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoTablePartitionedAtomicNear() throws Exception {
        checkCreateNoTable(PARTITIONED, ATOMIC, true);
    }

    /**
     * Test create when table doesn't exist for PARTITIONED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoTablePartitionedTransactional() throws Exception {
        checkCreateNoTable(PARTITIONED, TRANSACTIONAL, false);
    }

    /**
     * Test create when table doesn't exist for PARTITIONED TRANSACTIONAL cache with near cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoTablePartitionedTransactionalNear() throws Exception {
        checkCreateNoTable(PARTITIONED, TRANSACTIONAL, true);
    }

    /**
     * Test create when table doesn't exist for REPLICATED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoTableReplicatedAtomic() throws Exception {
        checkCreateNoTable(REPLICATED, ATOMIC, false);
    }

    /**
     * Test create when table doesn't exist for REPLICATED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoTableReplicatedTransactional() throws Exception {
        checkCreateNoTable(REPLICATED, TRANSACTIONAL, false);
    }

    /**
     * Check create when table doesn't exist.
     *
     * @param mode Mode.
     * @param atomicityMode Atomicity mode.
     * @param near Near flag.
     * @throws Exception If failed.
     */
    private void checkCreateNoTable(CacheMode mode, CacheAtomicityMode atomicityMode, boolean near) throws Exception {
        initialize(mode, atomicityMode, near);

        final QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1_ESCAPED));

        assertSchemaException(new RunnableX() {
            @Override public void run() throws Exception {
                dynamicIndexCreate(CACHE_NAME, randomString(), idx, false);
            }
        }, IgniteQueryErrorCode.TABLE_NOT_FOUND);

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);
    }

    /**
     * Test create when table doesn't exist for PARTITIONED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoColumnPartitionedAtomic() throws Exception {
        checkCreateNoColumn(PARTITIONED, ATOMIC, false);
    }

    /**
     * Test create when table doesn't exist for PARTITIONED ATOMIC cache with near cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoColumnPartitionedAtomicNear() throws Exception {
        checkCreateNoColumn(PARTITIONED, ATOMIC, true);
    }

    /**
     * Test create when table doesn't exist for PARTITIONED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoColumnPartitionedTransactional() throws Exception {
        checkCreateNoColumn(PARTITIONED, TRANSACTIONAL, false);
    }

    /**
     * Test create when table doesn't exist for PARTITIONED TRANSACTIONAL cache with near cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoColumnPartitionedTransactionalNear() throws Exception {
        checkCreateNoColumn(PARTITIONED, TRANSACTIONAL, true);
    }

    /**
     * Test create when table doesn't exist for REPLICATED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoColumnReplicatedAtomic() throws Exception {
        checkCreateNoColumn(REPLICATED, ATOMIC, false);
    }

    /**
     * Test create when table doesn't exist for REPLICATED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoColumnReplicatedTransactional() throws Exception {
        checkCreateNoColumn(REPLICATED, TRANSACTIONAL, false);
    }

    /**
     * Check create when table doesn't exist.
     *
     * @param mode Mode.
     * @param atomicityMode Atomicity mode.
     * @param near Near flag.
     * @throws Exception If failed.
     */
    private void checkCreateNoColumn(CacheMode mode, CacheAtomicityMode atomicityMode, boolean near) throws Exception {
        initialize(mode, atomicityMode, near);

        final QueryIndex idx = index(IDX_NAME_1, field(randomString()));

        assertSchemaException(new RunnableX() {
            @Override public void run() throws Exception {
                dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false);
            }
        }, IgniteQueryErrorCode.COLUMN_NOT_FOUND);

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);
    }

    /**
     * Test index creation on aliased column for PARTITIONED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateColumnWithAliasPartitionedAtomic() throws Exception {
        checkCreateColumnWithAlias(PARTITIONED, ATOMIC, false);
    }

    /**
     * Test index creation on aliased column for PARTITIONED ATOMIC cache with near cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateColumnWithAliasPartitionedAtomicNear() throws Exception {
        checkCreateColumnWithAlias(PARTITIONED, ATOMIC, true);
    }

    /**
     * Test index creation on aliased column for PARTITIONED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateColumnWithAliasPartitionedTransactional() throws Exception {
        checkCreateColumnWithAlias(PARTITIONED, TRANSACTIONAL, false);
    }

    /**
     * Test index creation on aliased column for PARTITIONED TRANSACTIONAL cache with near cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateColumnWithAliasPartitionedTransactionalNear() throws Exception {
        checkCreateColumnWithAlias(PARTITIONED, TRANSACTIONAL, true);
    }

    /**
     * Test index creation on aliased column for REPLICATED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateColumnWithAliasReplicatedAtomic() throws Exception {
        checkCreateColumnWithAlias(REPLICATED, ATOMIC, false);
    }

    /**
     * Test index creation on aliased column for REPLICATED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    public void testCreateColumnWithAliasReplicatedTransactional() throws Exception {
        checkCreateColumnWithAlias(REPLICATED, TRANSACTIONAL, false);
    }

    /**
     * Check index creation on aliased column.
     *
     * @param mode Mode.
     * @param atomicityMode Atomicity mode.
     * @param near Near flag.
     * @throws Exception If failed.
     */
    private void checkCreateColumnWithAlias(CacheMode mode, CacheAtomicityMode atomicityMode, boolean near)
        throws Exception {
        initialize(mode, atomicityMode, near);

        assertSchemaException(new RunnableX() {
            @Override public void run() throws Exception {
                QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_2_ESCAPED));

                dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false);
            }
        }, IgniteQueryErrorCode.COLUMN_NOT_FOUND);

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);

        QueryIndex idx = index(IDX_NAME_1, field(alias(FIELD_NAME_2_ESCAPED)));

        dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false);
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, field(alias(FIELD_NAME_2_ESCAPED)));

        assertSimpleIndexOperations(SQL_SIMPLE_FIELD_2);

        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_2, SQL_ARG_1);
    }

    /**
     * Test simple index drop for PARTITIONED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    public void testDropPartitionedAtomic() throws Exception {
        checkDrop(PARTITIONED, ATOMIC, false);
    }

    /**
     * Test simple index drop for PARTITIONED ATOMIC cache with near cache.
     *
     * @throws Exception If failed.
     */
    public void testDropPartitionedAtomicNear() throws Exception {
        checkDrop(PARTITIONED, ATOMIC, true);
    }

    /**
     * Test simple index drop for PARTITIONED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    public void testDropPartitionedTransactional() throws Exception {
        checkDrop(PARTITIONED, TRANSACTIONAL, false);
    }

    /**
     * Test simple index drop for PARTITIONED TRANSACTIONAL cache with near cache.
     *
     * @throws Exception If failed.
     */
    public void testDropPartitionedTransactionalNear() throws Exception {
        checkDrop(PARTITIONED, TRANSACTIONAL, true);
    }

    /**
     * Test simple index drop for REPLICATED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    public void testDropReplicatedAtomic() throws Exception {
        checkDrop(REPLICATED, ATOMIC, false);
    }

    /**
     * Test simple index drop for REPLICATED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    public void testDropReplicatedTransactional() throws Exception {
        checkDrop(REPLICATED, TRANSACTIONAL, false);
    }

    /**
     * Check simple index drop.
     *
     * @param mode Mode.
     * @param atomicityMode Atomicity mode.
     * @param near Near flag.
     * @throws Exception If failed.
     */
    public void checkDrop(CacheMode mode, CacheAtomicityMode atomicityMode, boolean near) throws Exception {
        initialize(mode, atomicityMode, near);

        // Create target index.
        QueryIndex idx1 = index(IDX_NAME_1, field(FIELD_NAME_1_ESCAPED));

        dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx1, false);
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, field(FIELD_NAME_1_ESCAPED));

        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_ARG_1);

        assertSimpleIndexOperations(SQL_SIMPLE_FIELD_1);

        // Create another index which must stay intact afterwards.
        QueryIndex idx2 = index(IDX_NAME_2, field(alias(FIELD_NAME_2_ESCAPED)));

        dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx2, false);
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_2, field(alias(FIELD_NAME_2_ESCAPED)));

        // Load some data.
        loadInitialData();

        // Drop index.
        dynamicIndexDrop(CACHE_NAME, IDX_NAME_1, false);
        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);

        assertSimpleIndexOperations(SQL_SIMPLE_FIELD_1);

        assertIndexNotUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_ARG_1);

        // Make sure the second index is still there.
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_2, field(alias(FIELD_NAME_2_ESCAPED)));
    }

    /**
     * Test drop when there is no index for PARTITIONED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    public void testDropNoIndexPartitionedAtomic() throws Exception {
        checkDropNoIndex(PARTITIONED, ATOMIC, false);
    }

    /**
     * Test drop when there is no index for PARTITIONED ATOMIC cache with near cache.
     *
     * @throws Exception If failed.
     */
    public void testDropNoIndexPartitionedAtomicNear() throws Exception {
        checkDropNoIndex(PARTITIONED, ATOMIC, true);
    }

    /**
     * Test drop when there is no index for PARTITIONED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    public void testDropNoIndexPartitionedTransactional() throws Exception {
        checkDropNoIndex(PARTITIONED, TRANSACTIONAL, false);
    }

    /**
     * Test drop when there is no index for PARTITIONED TRANSACTIONAL cache with near cache.
     *
     * @throws Exception If failed.
     */
    public void testDropNoIndexPartitionedTransactionalNear() throws Exception {
        checkDropNoIndex(PARTITIONED, TRANSACTIONAL, true);
    }

    /**
     * Test drop when there is no index for REPLICATED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    public void testDropNoIndexReplicatedAtomic() throws Exception {
        checkDropNoIndex(REPLICATED, ATOMIC, false);
    }

    /**
     * Test drop when there is no index for REPLICATED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    public void testDropNoIndexReplicatedTransactional() throws Exception {
        checkDropNoIndex(REPLICATED, TRANSACTIONAL, false);
    }

    /**
     * Check drop when there is no index.
     *
     * @param mode Mode.
     * @param atomicityMode Atomicity mode.
     * @param near Near flag.
     * @throws Exception If failed.
     */
    private void checkDropNoIndex(CacheMode mode, CacheAtomicityMode atomicityMode, boolean near) throws Exception {
        initialize(mode, atomicityMode, near);

        assertSchemaException(new RunnableX() {
            @Override public void run() throws Exception {
                dynamicIndexDrop(CACHE_NAME, IDX_NAME_1, false);
            }
        }, IgniteQueryErrorCode.INDEX_NOT_FOUND);

        dynamicIndexDrop(CACHE_NAME, IDX_NAME_1, true);
        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);
    }

    /**
     * Test drop when cache doesn't exist for PARTITIONED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    public void testDropNoCachePartitionedAtomic() throws Exception {
        checkDropNoCache(PARTITIONED, ATOMIC, false);
    }

    /**
     * Test drop when cache doesn't exist for PARTITIONED ATOMIC cache with near cache.
     *
     * @throws Exception If failed.
     */
    public void testDropNoCachePartitionedAtomicNear() throws Exception {
        checkDropNoCache(PARTITIONED, ATOMIC, true);
    }

    /**
     * Test drop when cache doesn't exist for PARTITIONED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    public void testDropNoCachePartitionedTransactional() throws Exception {
        checkDropNoCache(PARTITIONED, TRANSACTIONAL, false);
    }

    /**
     * Test drop when cache doesn't exist for PARTITIONED TRANSACTIONAL cache with near cache.
     *
     * @throws Exception If failed.
     */
    public void testDropNoCachePartitionedTransactionalNear() throws Exception {
        checkDropNoCache(PARTITIONED, TRANSACTIONAL, true);
    }

    /**
     * Test drop when cache doesn't exist for REPLICATED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    public void testDropNoCacheReplicatedAtomic() throws Exception {
        checkDropNoCache(REPLICATED, ATOMIC, false);
    }

    /**
     * Test drop when cache doesn't exist for REPLICATED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    public void testDropNoCacheReplicatedTransactional() throws Exception {
        checkDropNoCache(REPLICATED, TRANSACTIONAL, false);
    }

    /**
     * Check drop when cache doesn't exist.
     *
     * Check drop when there is no index.
     *
     * @param mode Mode.
     * @param atomicityMode Atomicity mode.
     * @param near Near flag.
     * @throws Exception If failed.
     */
    private void checkDropNoCache(CacheMode mode, CacheAtomicityMode atomicityMode, boolean near) throws Exception {
        initialize(mode, atomicityMode, near);

        try {
            String cacheName = randomString();

            queryProcessor(node()).dynamicIndexDrop(cacheName, cacheName, "my_idx", false).get();
        }
        catch (SchemaOperationException e) {
            assertEquals(SchemaOperationException.CODE_CACHE_NOT_FOUND, e.code());

            assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);

            return;
        }
        catch (Exception e) {
            fail("Unexpected exception: " + e);
        }

        fail(SchemaOperationException.class.getSimpleName() +  " is not thrown.");
    }

    /**
     * Test that operations fail on LOCAL cache.
     *
     * @throws Exception If failed.
     */
    public void testFailOnLocalCache() throws Exception {
        for (Ignite node : Ignition.allGrids()) {
            if (!node.configuration().isClientMode())
                node.getOrCreateCache(cacheConfiguration().setCacheMode(LOCAL));
        }

        final QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1_ESCAPED));

        assertSchemaException(new RunnableX() {
            @Override public void run() throws Exception {
                dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, true);
            }
        }, IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);

        assertSchemaException(new RunnableX() {
            @Override public void run() throws Exception {
                dynamicIndexDrop(CACHE_NAME, IDX_NAME_1, true);
            }
        }, IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
    }

    /**
     * Get node which should be used to start operations.
     *
     * @return If failed.
     */
    protected IgniteEx node() {
        return grid(nodeIndex());
    }

    /**
     * Get index of the node which should be used to start operations.
     *
     * @return If failed.
     */
    protected abstract int nodeIndex();

    /**
     * Get configurations to be used in test.
     *
     * @return Configurations.
     * @throws Exception If failed.
     */
    protected List<IgniteConfiguration> configurations() throws Exception {
        return Arrays.asList(
            serverCoordinatorConfiguration(IDX_SRV_CRD),
            serverConfiguration(IDX_SRV_NON_CRD),
            clientConfiguration(IDX_CLI),
            serverConfiguration(IDX_SRV_FILTERED, true),
            clientConfiguration(IDX_CLI_NEAR_ONLY)
        );
    }

    /**
     * Get server coordinator configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    protected IgniteConfiguration serverCoordinatorConfiguration(int idx) throws Exception {
        return serverConfiguration(idx);
    }

    /**
     * Assert FIELD_1 index usage.
     *
     * @param sql Simple SQL.
     */
    private void assertSimpleIndexOperations(String sql) {
        for (Ignite node : Ignition.allGrids())
            assertSqlSimpleData(node, sql, KEY_BEFORE - SQL_ARG_1);

        put(node(), KEY_BEFORE, KEY_AFTER);

        for (Ignite node : Ignition.allGrids())
            assertSqlSimpleData(node, sql, KEY_AFTER - SQL_ARG_1);

        remove(node(), 0, KEY_BEFORE);

        for (Ignite node : Ignition.allGrids())
            assertSqlSimpleData(node, sql, KEY_AFTER - KEY_BEFORE);

        remove(node(), KEY_BEFORE, KEY_AFTER);

        for (Ignite node : Ignition.allGrids())
            assertSqlSimpleData(node, sql, 0);
    }

    /**
     * Assert composite index usage.
     *
     * @param sql Simple SQL.
     */
    private void assertCompositeIndexOperations(String sql) {
        for (Ignite node : Ignition.allGrids())
            assertSqlCompositeData(node, sql, KEY_BEFORE - SQL_ARG_2);

        put(node(), KEY_BEFORE, KEY_AFTER);

        for (Ignite node : Ignition.allGrids())
            assertSqlCompositeData(node, sql, KEY_AFTER - SQL_ARG_2);

        remove(node(), 0, KEY_BEFORE);

        for (Ignite node : Ignition.allGrids())
            assertSqlCompositeData(node, sql, KEY_AFTER - KEY_BEFORE);

        remove(node(), KEY_BEFORE, KEY_AFTER);

        for (Ignite node : Ignition.allGrids())
            assertSqlCompositeData(node, sql, 0);
    }

    /**
     * Ensure that schema exception is thrown.
     *
     * @param r Runnable.
     * @param expCode Error code.
     */
    protected static void assertSchemaException(RunnableX r, int expCode) {
        try {
            r.run();
        }
        catch (CacheException e) {
            Throwable cause = e.getCause();

            assertTrue(cause != null);
            assertTrue("Unexpected cause: " + cause.getClass().getName(), cause instanceof IgniteSQLException);

            IgniteSQLException cause0 = (IgniteSQLException)cause;

            int code = cause0.statusCode();

            assertEquals("Unexpected error code [expected=" + expCode + ", actual=" + code +
                ", msg=" + cause.getMessage() + ']', expCode, code);

            return;
        }
        catch (Exception e) {
            fail("Unexpected exception: " + e);
        }

        fail(IgniteSQLException.class.getSimpleName() +  " is not thrown.");
    }

    /**
     * Synchronously create index.
     *
     * @param cacheName Cache name.
     * @param tblName Table name.
     * @param idx Index.
     * @param ifNotExists When set to true operation will fail if index already exists.
     * @throws Exception If failed.
     */
    private void dynamicIndexCreate(String cacheName, String tblName, QueryIndex idx, boolean ifNotExists)
        throws Exception {
        dynamicIndexCreate(node(), cacheName, tblName, idx, ifNotExists);
    }

    /**
     * Synchronously drop index.
     *
     * @param cacheName Cache name.
     * @param idxName Index name.
     * @param ifExists When set to true operation fill fail if index doesn't exists.
     * @throws Exception if failed.
     */
    private void dynamicIndexDrop(String cacheName, String idxName, boolean ifExists) throws Exception {
        dynamicIndexDrop(node(), cacheName, idxName, ifExists);
    }
}
