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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.testframework.GridTestUtils.RunnableX;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Tests for dynamic index creation.
 */
@SuppressWarnings({"unchecked"})
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

    /** Cache. */
    protected static final String STATIC_CACHE_NAME = "cache_static";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        for (IgniteConfiguration cfg : configurations())
            Ignition.start(cfg);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        node().context().cache().dynamicDestroyCache(CACHE_NAME, true, true, false, null).get();

        super.afterTest();
    }

    /**
     * Initialize cache for tests.
     *
     * @param mode Mode.
     * @param atomicityMode Atomicity mode.
     * @param near Near flag.
     * @throws Exception If failed.
     */
    private void initialize(CacheMode mode, CacheAtomicityMode atomicityMode, boolean near)
        throws Exception {
        createSqlCache(node(), cacheConfiguration(mode, atomicityMode, near));

        awaitPartitionMapExchange();

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
    @Test
    public void testCreatePartitionedAtomic() throws Exception {
        checkCreate(PARTITIONED, ATOMIC, false);
    }

    /**
     * Test simple index create for PARTITIONED ATOMIC cache with near cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreatePartitionedAtomicNear() throws Exception {
        checkCreate(PARTITIONED, ATOMIC, true);
    }

    /**
     * Test simple index create for PARTITIONED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreatePartitionedTransactional() throws Exception {
        checkCreate(PARTITIONED, TRANSACTIONAL, false);
    }

    /**
     * Test simple index create for PARTITIONED TRANSACTIONAL cache with near cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreatePartitionedTransactionalNear() throws Exception {
        checkCreate(PARTITIONED, TRANSACTIONAL, true);
    }

    /**
     * Test simple index create for REPLICATED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateReplicatedAtomic() throws Exception {
        checkCreate(REPLICATED, ATOMIC, false);
    }

    /**
     * Test simple index create for REPLICATED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
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

        dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false, 0);
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, QueryIndex.DFLT_INLINE_SIZE, field(FIELD_NAME_1_ESCAPED));

        assertIgniteSqlException(new RunnableX() {
            @Override public void runx() throws Exception {
                dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false, 0);
            }
        }, IgniteQueryErrorCode.INDEX_ALREADY_EXISTS);

        dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, true, 0);
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, QueryIndex.DFLT_INLINE_SIZE, field(FIELD_NAME_1_ESCAPED));

        assertSimpleIndexOperations(SQL_SIMPLE_FIELD_1);

        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_ARG_1);
    }

    /**
     * Test composite index creation for PARTITIONED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateCompositePartitionedAtomic() throws Exception {
        checkCreateComposite(PARTITIONED, ATOMIC, false);
    }

    /**
     * Test composite index creation for PARTITIONED ATOMIC cache with near cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateCompositePartitionedAtomicNear() throws Exception {
        checkCreateComposite(PARTITIONED, ATOMIC, true);
    }

    /**
     * Test composite index creation for PARTITIONED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateCompositePartitionedTransactional() throws Exception {
        checkCreateComposite(PARTITIONED, TRANSACTIONAL, false);
    }

    /**
     * Test composite index creation for PARTITIONED TRANSACTIONAL cache with near cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateCompositePartitionedTransactionalNear() throws Exception {
        checkCreateComposite(PARTITIONED, TRANSACTIONAL, true);
    }

    /**
     * Test composite index creation for REPLICATED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateCompositeReplicatedAtomic() throws Exception {
        checkCreateComposite(REPLICATED, ATOMIC, false);
    }

    /**
     * Test composite index creation for REPLICATED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
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

        dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false, 0);
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, QueryIndex.DFLT_INLINE_SIZE,
            field(FIELD_NAME_1_ESCAPED), field(alias(FIELD_NAME_2_ESCAPED)));

        assertCompositeIndexOperations(SQL_COMPOSITE);

        assertIndexUsed(IDX_NAME_1, SQL_COMPOSITE, SQL_ARG_1, SQL_ARG_2);
    }

    /**
     * Test create when cache doesn't exist for PARTITIONED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateIndexNoCachePartitionedAtomic() throws Exception {
        checkCreateNotCache(PARTITIONED, ATOMIC, false);
    }

    /**
     * Test create when cache doesn't exist for PARTITIONED ATOMIC cache with near cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateIndexNoCachePartitionedAtomicNear() throws Exception {
        checkCreateNotCache(PARTITIONED, ATOMIC, true);
    }

    /**
     * Test create when cache doesn't exist for PARTITIONED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateIndexNoCachePartitionedTransactional() throws Exception {
        checkCreateNotCache(PARTITIONED, TRANSACTIONAL, false);
    }

    /**
     * Test create when cache doesn't exist for PARTITIONED TRANSACTIONAL cache with near cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateIndexNoCachePartitionedTransactionalNear() throws Exception {
        checkCreateNotCache(PARTITIONED, TRANSACTIONAL, true);
    }

    /**
     * Test create when cache doesn't exist for REPLICATED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateIndexNoCacheReplicatedAtomic() throws Exception {
        checkCreateNotCache(REPLICATED, ATOMIC, false);
    }

    /**
     * Test create when cache doesn't exist for REPLICATED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateIndexNoCacheReplicatedTransactional() throws Exception {
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

            queryProcessor(node()).dynamicIndexCreate(cacheName, cacheName, TBL_NAME, idx, false, 0).get();
        }
        catch (SchemaOperationException e) {
            assertEquals(SchemaOperationException.CODE_CACHE_NOT_FOUND, e.code());

            assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);

            return;
        }
        catch (Exception e) {
            fail("Unexpected exception: " + e);
        }

        fail(SchemaOperationException.class.getSimpleName() + " is not thrown.");
    }

    /**
     * Test create when table doesn't exist for PARTITIONED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateNoTablePartitionedAtomic() throws Exception {
        checkCreateNoTable(PARTITIONED, ATOMIC, false);
    }

    /**
     * Test create when table doesn't exist for PARTITIONED ATOMIC cache with near cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateNoTablePartitionedAtomicNear() throws Exception {
        checkCreateNoTable(PARTITIONED, ATOMIC, true);
    }

    /**
     * Test create when table doesn't exist for PARTITIONED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateNoTablePartitionedTransactional() throws Exception {
        checkCreateNoTable(PARTITIONED, TRANSACTIONAL, false);
    }

    /**
     * Test create when table doesn't exist for PARTITIONED TRANSACTIONAL cache with near cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateNoTablePartitionedTransactionalNear() throws Exception {
        checkCreateNoTable(PARTITIONED, TRANSACTIONAL, true);
    }

    /**
     * Test create when table doesn't exist for REPLICATED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateNoTableReplicatedAtomic() throws Exception {
        checkCreateNoTable(REPLICATED, ATOMIC, false);
    }

    /**
     * Test create when table doesn't exist for REPLICATED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
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

        assertIgniteSqlException(new RunnableX() {
            @Override public void runx() throws Exception {
                dynamicIndexCreate(CACHE_NAME, randomString(), idx, false, 0);
            }
        }, IgniteQueryErrorCode.TABLE_NOT_FOUND);

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);
    }

    /**
     * Test create when table doesn't exist for PARTITIONED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateIndexNoColumnPartitionedAtomic() throws Exception {
        checkCreateIndexNoColumn(PARTITIONED, ATOMIC, false);
    }

    /**
     * Test create when table doesn't exist for PARTITIONED ATOMIC cache with near cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateIndexNoColumnPartitionedAtomicNear() throws Exception {
        checkCreateIndexNoColumn(PARTITIONED, ATOMIC, true);
    }

    /**
     * Test create when table doesn't exist for PARTITIONED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateIndexNoColumnPartitionedTransactional() throws Exception {
        checkCreateIndexNoColumn(PARTITIONED, TRANSACTIONAL, false);
    }

    /**
     * Test create when table doesn't exist for PARTITIONED TRANSACTIONAL cache with near cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateIndexNoColumnPartitionedTransactionalNear() throws Exception {
        checkCreateIndexNoColumn(PARTITIONED, TRANSACTIONAL, true);
    }

    /**
     * Test create when table doesn't exist for REPLICATED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateIndexNoColumnReplicatedAtomic() throws Exception {
        checkCreateIndexNoColumn(REPLICATED, ATOMIC, false);
    }

    /**
     * Test create when table doesn't exist for REPLICATED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateIndexNoColumnReplicatedTransactional() throws Exception {
        checkCreateIndexNoColumn(REPLICATED, TRANSACTIONAL, false);
    }

    /**
     * Check create when table doesn't exist.
     *
     * @param mode Mode.
     * @param atomicityMode Atomicity mode.
     * @param near Near flag.
     * @throws Exception If failed.
     */
    private void checkCreateIndexNoColumn(CacheMode mode, CacheAtomicityMode atomicityMode, boolean near) throws Exception {
        initialize(mode, atomicityMode, near);

        final QueryIndex idx = index(IDX_NAME_1, field(randomString()));

        assertIgniteSqlException(new RunnableX() {
            @Override public void runx() throws Exception {
                dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false, 0);
            }
        }, IgniteQueryErrorCode.COLUMN_NOT_FOUND);

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);
    }

    /**
     * Test index creation on aliased column for PARTITIONED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateIndexOnColumnWithAliasPartitionedAtomic() throws Exception {
        checkCreateIndexOnColumnWithAlias(PARTITIONED, ATOMIC, false);
    }

    /**
     * Test index creation on aliased column for PARTITIONED ATOMIC cache with near cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateIndexOnColumnWithAliasPartitionedAtomicNear() throws Exception {
        checkCreateIndexOnColumnWithAlias(PARTITIONED, ATOMIC, true);
    }

    /**
     * Test index creation on aliased column for PARTITIONED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateIndexOnColumnWithAliasPartitionedTransactional() throws Exception {
        checkCreateIndexOnColumnWithAlias(PARTITIONED, TRANSACTIONAL, false);
    }

    /**
     * Test index creation on aliased column for PARTITIONED TRANSACTIONAL cache with near cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateColumnWithAliasPartitionedTransactionalNear() throws Exception {
        checkCreateIndexOnColumnWithAlias(PARTITIONED, TRANSACTIONAL, true);
    }

    /**
     * Test index creation on aliased column for REPLICATED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateColumnWithAliasReplicatedAtomic() throws Exception {
        checkCreateIndexOnColumnWithAlias(REPLICATED, ATOMIC, false);
    }

    /**
     * Test index creation on aliased column for REPLICATED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCreateColumnWithAliasReplicatedTransactional() throws Exception {
        checkCreateIndexOnColumnWithAlias(REPLICATED, TRANSACTIONAL, false);
    }

    /**
     * Check index creation on aliased column.
     *
     * @param mode Mode.
     * @param atomicityMode Atomicity mode.
     * @param near Near flag.
     * @throws Exception If failed.
     */
    private void checkCreateIndexOnColumnWithAlias(CacheMode mode, CacheAtomicityMode atomicityMode, boolean near)
        throws Exception {
        initialize(mode, atomicityMode, near);

        assertIgniteSqlException(new RunnableX() {
            @Override public void runx() throws Exception {
                QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_2_ESCAPED));

                dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false, 0);
            }
        }, IgniteQueryErrorCode.COLUMN_NOT_FOUND);

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);

        QueryIndex idx = index(IDX_NAME_1, field(alias(FIELD_NAME_2_ESCAPED)));

        dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false, 0);
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, QueryIndex.DFLT_INLINE_SIZE, field(alias(FIELD_NAME_2_ESCAPED)));

        assertSimpleIndexOperations(SQL_SIMPLE_FIELD_2);

        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_2, SQL_ARG_1);
    }

    /**
     * Tests creating index with inline size for PARTITIONED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK, value = "true")
    public void testCreateIndexWithInlineSizePartitionedAtomic() throws Exception {
        checkCreateIndexWithInlineSize(PARTITIONED, ATOMIC, false);
    }

    /**
     * Tests creating index with inline size for PARTITIONED ATOMIC cache with near cache.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK, value = "true")
    public void testCreateIndexWithInlineSizePartitionedAtomicNear() throws Exception {
        checkCreateIndexWithInlineSize(PARTITIONED, ATOMIC, true);
    }

    /**
     * Tests creating index with inline size for PARTITIONED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK, value = "true")
    public void testCreateIndexWithInlineSizePartitionedTransactional() throws Exception {
        checkCreateIndexWithInlineSize(PARTITIONED, TRANSACTIONAL, false);
    }

    /**
     * Tests creating index with inline size for PARTITIONED TRANSACTIONAL cache with near cache.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK, value = "true")
    public void testCreateIndexWithInlineSizePartitionedTransactionalNear() throws Exception {
        checkCreateIndexWithInlineSize(PARTITIONED, TRANSACTIONAL, true);
    }

    /**
     * Tests creating index with inline size for REPLICATED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK, value = "true")
    public void testCreateIndexWithInlineSizeReplicatedAtomic() throws Exception {
        checkCreateIndexWithInlineSize(REPLICATED, ATOMIC, false);
    }

    /**
     * Tests creating index with inline size option for REPLICATED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK, value = "true")
    public void testCreateIndexWithInlineSizeReplicatedTransactional() throws Exception {
        checkCreateIndexWithInlineSize(REPLICATED, TRANSACTIONAL, false);
    }

    /**
     * Checks that inline size parameter is correctly handled during index creation.
     *
     * @param mode Mode.
     * @param atomicityMode Atomicity mode.
     * @param near Near flag.
     * @throws Exception If failed.
     */
    private void checkCreateIndexWithInlineSize(CacheMode mode, CacheAtomicityMode atomicityMode, boolean near)
        throws Exception {

        initialize(mode, atomicityMode, near);

        checkNoIndexIsCreatedForInlineSize(-2, IgniteQueryErrorCode.PARSING);
        checkNoIndexIsCreatedForInlineSize(Integer.MIN_VALUE, IgniteQueryErrorCode.PARSING);

        checkIndexCreatedForInlineSize(0);
        loadInitialData();
        checkIndexCreatedForInlineSize(1);
        loadInitialData();
        checkIndexCreatedForInlineSize(Integer.MAX_VALUE);
    }

    /**
     * Verifies that index is created with the specified inline size.
     *
     * @param inlineSize Inline size to put into CREATE INDEX
     * @throws Exception If failed.
     */
    private void checkIndexCreatedForInlineSize(int inlineSize) throws Exception {
        QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1_ESCAPED));
        idx.setInlineSize(inlineSize);

        dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false, 0);

        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, inlineSize, field(FIELD_NAME_1_ESCAPED));
        assertSimpleIndexOperations(SQL_SIMPLE_FIELD_1);
        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_ARG_1);

        dynamicIndexDrop(CACHE_NAME, IDX_NAME_1, false);

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);
    }

    /**
     * Verifies that no index is created and an exception is thrown.
     *
     * @param inlineSize Inline size value in the CREATE INDEX statement.
     * @param igniteQryErrorCode Expected error code in the thrown exception.
     * @throws Exception If failed for any other reason than the expected exception.
     */
    private void checkNoIndexIsCreatedForInlineSize(final int inlineSize, int igniteQryErrorCode) throws Exception {
        assertIgniteSqlException(new RunnableX() {
            @Override public void runx() throws Exception {
                QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1_ESCAPED));
                idx.setInlineSize(inlineSize);
                dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false, 0);
            }
        }, igniteQryErrorCode);

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);
    }


    /**
     * Tests creating index with parallelism for PARTITIONED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK, value = "true")
    public void testCreateIndexWithParallelismPartitionedAtomic() throws Exception {
        checkCreateIndexWithParallelism(PARTITIONED, ATOMIC, false);
    }

    /**
     * Tests creating index with parallelism for PARTITIONED ATOMIC cache with near cache.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK, value = "true")
    public void testCreateIndexWithParallelismPartitionedAtomicNear() throws Exception {
        checkCreateIndexWithParallelism(PARTITIONED, ATOMIC, true);
    }

    /**
     * Tests creating index with parallelism for PARTITIONED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK, value = "true")
    public void testCreateIndexWithParallelismPartitionedTransactional() throws Exception {
        checkCreateIndexWithParallelism(PARTITIONED, TRANSACTIONAL, false);
    }

    /**
     * Tests creating index with parallelism for PARTITIONED TRANSACTIONAL cache with near cache.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK, value = "true")
    public void testCreateIndexWithParallelismPartitionedTransactionalNear() throws Exception {
        checkCreateIndexWithParallelism(PARTITIONED, TRANSACTIONAL, true);
    }

    /**
     * Tests creating index with parallelism for REPLICATED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK, value = "true")
    public void testCreateIndexWithParallelismReplicatedAtomic() throws Exception {
        checkCreateIndexWithParallelism(REPLICATED, ATOMIC, false);
    }

    /**
     * Tests creating index with parallelism option for REPLICATED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK, value = "true")
    public void testCreateIndexWithParallelismReplicatedTransactional() throws Exception {
        checkCreateIndexWithParallelism(REPLICATED, TRANSACTIONAL, false);
    }

    /**
     * Checks that parallelism parameter is correctly handled during index creation.
     *
     * @param mode Mode.
     * @param atomicityMode Atomicity mode.
     * @param near Near flag.
     * @throws Exception If failed.
     */
    private void checkCreateIndexWithParallelism(CacheMode mode, CacheAtomicityMode atomicityMode, boolean near)
        throws Exception {

        initialize(mode, atomicityMode, near);

        checkNoIndexIsCreatedForParallelism(-2, IgniteQueryErrorCode.PARSING);
        checkNoIndexIsCreatedForParallelism(Integer.MIN_VALUE, IgniteQueryErrorCode.PARSING);

        checkIndexCreatedForParallelism(0);
        loadInitialData();
        checkIndexCreatedForParallelism(1);
        loadInitialData();
        checkIndexCreatedForParallelism(5);
    }

    /**
     * Verifies that index was created properly with different parallelism levels.
     * NOTE! Unfortunately we cannot check the real parallelism level on which this index was created because it should
     * use internal API. But we can check if this index was created properly on different parallelism levels.
     *
     * @param parallel Parallelism level to put into CREATE INDEX
     * @throws Exception If failed.
     */
    private void checkIndexCreatedForParallelism(int parallel) throws Exception {
        QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1_ESCAPED));

        dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false, parallel);

        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, QueryIndex.DFLT_INLINE_SIZE, field(FIELD_NAME_1_ESCAPED));
        assertSimpleIndexOperations(SQL_SIMPLE_FIELD_1);
        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_ARG_1);

        dynamicIndexDrop(CACHE_NAME, IDX_NAME_1, false);

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);
    }

    /**
     * Verifies that no index is created and an exception is thrown.
     *
     * @param parallel Parallelism level in the CREATE INDEX statement.
     * @param igniteQryErrorCode Expected error code in the thrown exception.
     * @throws Exception If failed for any other reason than the expected exception.
     */
    private void checkNoIndexIsCreatedForParallelism(final int parallel, int igniteQryErrorCode) throws Exception {
        assertIgniteSqlException(new RunnableX() {
            @Override public void runx() throws Exception {
                QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1_ESCAPED));
                dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false, parallel);
            }
        }, igniteQryErrorCode);

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);
    }

    /**
     * Test simple index drop for PARTITIONED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDropPartitionedAtomic() throws Exception {
        checkDrop(PARTITIONED, ATOMIC, false);
    }

    /**
     * Test simple index drop for PARTITIONED ATOMIC cache with near cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDropPartitionedAtomicNear() throws Exception {
        checkDrop(PARTITIONED, ATOMIC, true);
    }

    /**
     * Test simple index drop for PARTITIONED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDropPartitionedTransactional() throws Exception {
        checkDrop(PARTITIONED, TRANSACTIONAL, false);
    }

    /**
     * Test simple index drop for PARTITIONED TRANSACTIONAL cache with near cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDropPartitionedTransactionalNear() throws Exception {
        checkDrop(PARTITIONED, TRANSACTIONAL, true);
    }

    /**
     * Test simple index drop for REPLICATED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDropReplicatedAtomic() throws Exception {
        checkDrop(REPLICATED, ATOMIC, false);
    }

    /**
     * Test simple index drop for REPLICATED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
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

        dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx1, false, 0);
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, QueryIndex.DFLT_INLINE_SIZE, field(FIELD_NAME_1_ESCAPED));

        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_ARG_1);

        assertSimpleIndexOperations(SQL_SIMPLE_FIELD_1);

        // Create another index which must stay intact afterwards.
        QueryIndex idx2 = index(IDX_NAME_2, field(alias(FIELD_NAME_2_ESCAPED)));

        dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx2, false, 0);
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_2, QueryIndex.DFLT_INLINE_SIZE, field(alias(FIELD_NAME_2_ESCAPED)));

        // Load some data.
        loadInitialData();

        // Drop index.
        dynamicIndexDrop(CACHE_NAME, IDX_NAME_1, false);
        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);

        assertSimpleIndexOperations(SQL_SIMPLE_FIELD_1);

        assertIndexNotUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_ARG_1);

        // Make sure the second index is still there.
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_2, QueryIndex.DFLT_INLINE_SIZE, field(alias(FIELD_NAME_2_ESCAPED)));
    }

    /**
     * Test drop when there is no index for PARTITIONED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDropNoIndexPartitionedAtomic() throws Exception {
        checkDropNoIndex(PARTITIONED, ATOMIC, false);
    }

    /**
     * Test drop when there is no index for PARTITIONED ATOMIC cache with near cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDropNoIndexPartitionedAtomicNear() throws Exception {
        checkDropNoIndex(PARTITIONED, ATOMIC, true);
    }

    /**
     * Test drop when there is no index for PARTITIONED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDropNoIndexPartitionedTransactional() throws Exception {
        checkDropNoIndex(PARTITIONED, TRANSACTIONAL, false);
    }

    /**
     * Test drop when there is no index for PARTITIONED TRANSACTIONAL cache with near cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDropNoIndexPartitionedTransactionalNear() throws Exception {
        checkDropNoIndex(PARTITIONED, TRANSACTIONAL, true);
    }

    /**
     * Test drop when there is no index for REPLICATED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDropNoIndexReplicatedAtomic() throws Exception {
        checkDropNoIndex(REPLICATED, ATOMIC, false);
    }

    /**
     * Test drop when there is no index for REPLICATED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
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

        assertIgniteSqlException(new RunnableX() {
            @Override public void runx() throws Exception {
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
    @Test
    public void testDropNoCachePartitionedAtomic() throws Exception {
        checkDropNoCache(PARTITIONED, ATOMIC, false);
    }

    /**
     * Test drop when cache doesn't exist for PARTITIONED ATOMIC cache with near cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDropNoCachePartitionedAtomicNear() throws Exception {
        checkDropNoCache(PARTITIONED, ATOMIC, true);
    }

    /**
     * Test drop when cache doesn't exist for PARTITIONED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDropNoCachePartitionedTransactional() throws Exception {
        checkDropNoCache(PARTITIONED, TRANSACTIONAL, false);
    }

    /**
     * Test drop when cache doesn't exist for PARTITIONED TRANSACTIONAL cache with near cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDropNoCachePartitionedTransactionalNear() throws Exception {
        checkDropNoCache(PARTITIONED, TRANSACTIONAL, true);
    }

    /**
     * Test drop when cache doesn't exist for REPLICATED ATOMIC cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDropNoCacheReplicatedAtomic() throws Exception {
        checkDropNoCache(REPLICATED, ATOMIC, false);
    }

    /**
     * Test drop when cache doesn't exist for REPLICATED TRANSACTIONAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
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

        fail(SchemaOperationException.class.getSimpleName() + " is not thrown.");
    }

    /**
     * Test that operations fail on LOCAL cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testFailOnLocalCache() throws Exception {
        for (Ignite node : Ignition.allGrids()) {
            if (!node.configuration().isClientMode())
                createSqlCache(node, localCacheConfiguration());
        }

        final QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1_ESCAPED));

        assertIgniteSqlException(new RunnableX() {
            @Override public void runx() throws Exception {
                dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, true, 0);
            }
        }, IgniteQueryErrorCode.UNSUPPORTED_OPERATION);

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);

        assertIgniteSqlException(new RunnableX() {
            @Override public void runx() throws Exception {
                dynamicIndexDrop(CACHE_NAME, IDX_NAME_LOCAL, true);
            }
        }, IgniteQueryErrorCode.UNSUPPORTED_OPERATION);
    }

    /**
     * Test that operations work on statically configured cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNonSqlCache() throws Exception {
        final QueryIndex idx = index(IDX_NAME_2, field(FIELD_NAME_1));

        dynamicIndexCreate(STATIC_CACHE_NAME, TBL_NAME, idx, true, 0);
        assertIndex(STATIC_CACHE_NAME, TBL_NAME, IDX_NAME_1, QueryIndex.DFLT_INLINE_SIZE, field(FIELD_NAME_1_ESCAPED));

        dynamicIndexDrop(STATIC_CACHE_NAME, IDX_NAME_1, true);
        assertNoIndex(STATIC_CACHE_NAME, TBL_NAME, IDX_NAME_1);
    }

    /**
     * Test behavior depending on index name case sensitivity.
     */
    @Test
    public void testIndexNameCaseSensitivity() throws Exception {
        doTestIndexNameCaseSensitivity("myIdx", false);

        doTestIndexNameCaseSensitivity("myIdx", true);
    }

    /**
     * Perform a check on given index name considering case sensitivity.
     * @param idxName Index name to check.
     * @param sensitive Whether index should be created w/case sensitive name or not.
     */
    private void doTestIndexNameCaseSensitivity(String idxName, boolean sensitive) throws Exception {
        String idxNameSql = (sensitive ? '"' + idxName + '"' : idxName);

        // This one should always work.
        assertIndexNameIsValid(idxNameSql, idxNameSql);

        if (sensitive) {
            assertIndexNameIsNotValid(idxNameSql, idxName.toUpperCase());

            assertIndexNameIsNotValid(idxNameSql, idxName.toLowerCase());
        }
        else {
            assertIndexNameIsValid(idxNameSql, '"' + idxName.toUpperCase() + '"');

            assertIndexNameIsValid(idxNameSql, idxName.toUpperCase());

            assertIndexNameIsValid(idxNameSql, idxName.toLowerCase());
        }
    }

    /**
     * Check that given variant of index name works for DDL context.
     * @param idxNameToCreate Name of the index to use in {@code CREATE INDEX}.
     * @param checkedIdxName Index name to use in actual check.
     */
    private void assertIndexNameIsValid(String idxNameToCreate, String checkedIdxName) throws Exception {
        info("Checking index name variant for validity: " + checkedIdxName);

        final QueryIndex idx = index(idxNameToCreate, field(FIELD_NAME_1));

        dynamicIndexCreate(STATIC_CACHE_NAME, TBL_NAME, idx, true, 0);

        dynamicIndexDrop(STATIC_CACHE_NAME, checkedIdxName, false);
    }

    /**
     * Check that given variant of index name works for DDL context.
     * @param idxNameToCreate Name of the index to use in {@code CREATE INDEX}.
     * @param checkedIdxName Index name to use in actual check.
     */
    private void assertIndexNameIsNotValid(String idxNameToCreate, final String checkedIdxName) throws Exception {
        info("Checking index name variant for invalidity: " + checkedIdxName);

        final QueryIndex idx = index(idxNameToCreate, field(FIELD_NAME_1));

        dynamicIndexCreate(STATIC_CACHE_NAME, TBL_NAME, idx, true, 0);

        assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                dynamicIndexDrop(STATIC_CACHE_NAME, checkedIdxName, false);

                return null;
            }
        }, IgniteSQLException.class, "Index doesn't exist: " + checkedIdxName.toUpperCase());
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

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration commonConfiguration(int idx) throws Exception {
        IgniteConfiguration cfg = super.commonConfiguration(idx);

        if (idx != nodeIndex())
            return cfg;

        CacheConfiguration staticCacheCfg = cacheConfiguration().setName(STATIC_CACHE_NAME);

        ((QueryEntity)staticCacheCfg.getQueryEntities().iterator().next()).setIndexes(Collections.singletonList(index(
            IDX_NAME_1, field(FIELD_NAME_1)
        )));

        CacheConfiguration[] newCfgs = new CacheConfiguration[F.isEmpty(cfg.getCacheConfiguration()) ? 1 :
            cfg.getCacheConfiguration().length + 1];

        if (newCfgs.length > 1)
            System.arraycopy(cfg.getCacheConfiguration(), 0, newCfgs, 0, newCfgs.length - 1);

        newCfgs[newCfgs.length - 1] = staticCacheCfg;

        cfg.setCacheConfiguration(newCfgs);

        return cfg;
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
    protected static void assertIgniteSqlException(Runnable r, int expCode) {
        assertIgniteSqlException(r, null, expCode);
    }

    /**
     * Ensure that schema exception is thrown.
     *
     * @param r Runnable.
     * @param msg Exception message to expect, or {@code null} if it can be waived.
     * @param expCode Error code.
     */
    private static void assertIgniteSqlException(Runnable r, String msg, int expCode) {
        try {
            r.run();
        }
        catch (IgniteException ie) {
            assertTrue("Unexpected exception: " + ie, ie.getCause() instanceof CacheException);

            checkCacheException(msg, expCode, (CacheException)ie.getCause());

            return;
        }
        catch (CacheException e) {
            checkCacheException(msg, expCode, e);

            return;
        }
        catch (Exception e) {
            fail("Unexpected exception: " + e);
        }

        fail(IgniteSQLException.class.getSimpleName() + " is not thrown.");
    }

    /** */
    private static void checkCacheException(String msg, int expCode, CacheException e) {
        Throwable cause = e.getCause();

        assertTrue(cause != null);
        assertTrue("Unexpected cause: " + cause.getClass().getName(), cause instanceof IgniteSQLException);

        IgniteSQLException cause0 = (IgniteSQLException)cause;

        int code = cause0.statusCode();

        assertEquals("Unexpected error code [expected=" + expCode + ", actual=" + code +
            ", msg=" + cause.getMessage() + ']', expCode, code);

        if (msg != null)
            assertEquals("Unexpected error message [expected=" + msg + ", actual=" + cause0.getMessage() + ']',
                msg, cause0.getMessage());
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
    private void dynamicIndexCreate(String cacheName, String tblName, QueryIndex idx, boolean ifNotExists, int parallel)
        throws Exception {
        dynamicIndexCreate(node(), cacheName, tblName, idx, ifNotExists, parallel);
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
