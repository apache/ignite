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
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.schema.SchemaOperationException;

import java.util.Arrays;
import java.util.List;

/**
 * Tests for dynamic index creation.
 */
@SuppressWarnings({"unchecked", "ThrowableResultOfMethodCallIgnored"})
public abstract class DynamicIndexBasicAbstractSelfTest extends DynamicIndexAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        for (IgniteConfiguration cfg : configurations())
            Ignition.start(cfg);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        node().getOrCreateCache(cacheConfiguration());

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);

        loadInitialData();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        node().destroyCache(CACHE_NAME);

        super.afterTest();
    }

    /**
     * Load initial data.
     */
    private void loadInitialData() {
        put(node(), 0, KEY_BEFORE);
    }

    /**
     * Test simple index create.
     *
     * @throws Exception If failed.
     */
    public void testCreate() throws Exception {
        final IgniteEx node = node();

        final QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1));

        queryProcessor(node).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false).get();
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, field(FIELD_NAME_1));

        assertSchemaException(new RunnableX() {
            @Override public void run() throws Exception {
                queryProcessor(node).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false).get();
            }
        }, SchemaOperationException.CODE_INDEX_EXISTS);

        queryProcessor(node).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, true).get();
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, field(FIELD_NAME_1));

        assertSimpleIndexOperations(SQL_SIMPLE_FIELD_1);

        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_SIMPLE_ARG);
    }

    /**
     * Test composite index creation.
     *
     * @throws Exception If failed.
     */
    public void testCreateComposite() throws Exception {
        final QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1), field(alias(FIELD_NAME_2)));

        queryProcessor(node()).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false).get();
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, field(FIELD_NAME_1), field(alias(FIELD_NAME_2)));

        // TODO
    }

    /**
     * Test create when cache doesn't exist.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoCache() throws Exception {
        final QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1));

        assertSchemaException(new RunnableX() {
            @Override public void run() throws Exception {
                queryProcessor(node()).dynamicIndexCreate(randomString(), TBL_NAME, idx, false).get();
            }
        }, SchemaOperationException.CODE_CACHE_NOT_FOUND);

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);
    }

    /**
     * Test create when table doesn't exist.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoTable() throws Exception {
        final QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1));

        assertSchemaException(new RunnableX() {
            @Override public void run() throws Exception {
                queryProcessor(node()).dynamicIndexCreate(CACHE_NAME, randomString(), idx, false).get();
            }
        }, SchemaOperationException.CODE_TABLE_NOT_FOUND);

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);
    }

    /**
     * Test create when table doesn't exist.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoColumn() throws Exception {
        final QueryIndex idx = index(IDX_NAME_1, field(randomString()));

        assertSchemaException(new RunnableX() {
            @Override public void run() throws Exception {
                queryProcessor(node()).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false).get();
            }
        }, SchemaOperationException.CODE_COLUMN_NOT_FOUND);

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);
    }

    /**
     * Test index creation on aliased column.
     *
     * @throws Exception If failed.
     */
    public void testCreateColumnWithAlias() throws Exception {
        assertSchemaException(new RunnableX() {
            @Override public void run() throws Exception {
                QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_2));

                queryProcessor(node()).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false).get();
            }
        }, SchemaOperationException.CODE_COLUMN_NOT_FOUND);

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);

        QueryIndex idx = index(IDX_NAME_1, field(alias(FIELD_NAME_2)));

        queryProcessor(node()).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false).get();
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, field(alias(FIELD_NAME_2)));

        assertSimpleIndexOperations(SQL_SIMPLE_FIELD_2);

        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_2, SQL_SIMPLE_ARG);
    }

    /**
     * Test simple index drop.
     *
     * @throws Exception If failed.
     */
    public void testDrop() throws Exception {
        QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1));

        queryProcessor(node()).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false).get();
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1, field(FIELD_NAME_1));

        assertIndexUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_SIMPLE_ARG);

        assertSimpleIndexOperations(SQL_SIMPLE_FIELD_1);

        loadInitialData();

        queryProcessor(node()).dynamicIndexDrop(CACHE_NAME, IDX_NAME_1, false).get();
        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);

        assertSimpleIndexOperations(SQL_SIMPLE_FIELD_1);

        assertIndexNotUsed(IDX_NAME_1, SQL_SIMPLE_FIELD_1, SQL_SIMPLE_ARG);
    }

    /**
     * Test drop when there is no index.
     *
     * @throws Exception If failed.
     */
    public void testDropNoIndex() throws Exception {
        assertSchemaException(new RunnableX() {
            @Override public void run() throws Exception {
                queryProcessor(node()).dynamicIndexDrop(CACHE_NAME, IDX_NAME_1, false).get();
            }
        }, SchemaOperationException.CODE_INDEX_NOT_FOUND);

        queryProcessor(node()).dynamicIndexDrop(CACHE_NAME, IDX_NAME_1, true).get();
        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);
    }

    /**
     * Test drop when cache doesn't exist.
     *
     * @throws Exception If failed.
     */
    public void testDropNoCache() throws Exception {
        assertSchemaException(new RunnableX() {
            @Override public void run() throws Exception {
                queryProcessor(node()).dynamicIndexDrop(randomString(), "my_idx", false).get();
            }
        }, SchemaOperationException.CODE_CACHE_NOT_FOUND);

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME_1);
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
            serverConfiguration(0),
            serverConfiguration(1),
            clientConfiguration(2),
            serverConfiguration(3, true)
        );
    }

    /**
     * Assert FIELD_1 index usage.
     *
     * @param sql Simple SQL.
     */
    private void assertSimpleIndexOperations(String sql) {
        for (Ignite node : Ignition.allGrids())
            assertSqlSimpleData((IgniteEx)node, sql, KEY_BEFORE - SQL_SIMPLE_ARG);

        put(node(), KEY_BEFORE, KEY_AFTER);

        for (Ignite node : Ignition.allGrids())
            assertSqlSimpleData((IgniteEx)node, sql, KEY_AFTER - SQL_SIMPLE_ARG);

        remove(node(), 0, KEY_BEFORE);

        for (Ignite node : Ignition.allGrids())
            assertSqlSimpleData((IgniteEx)node, sql, KEY_AFTER - KEY_BEFORE);

        remove(node(), KEY_BEFORE, KEY_AFTER);

        for (Ignite node : Ignition.allGrids())
            assertSqlSimpleData((IgniteEx)node, sql, 0);
    }
}
