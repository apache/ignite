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

import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.query.index.SchemaOperationException;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Tests for dynamic index creation.
 */
@SuppressWarnings({"unchecked", "ThrowableResultOfMethodCallIgnored"})
public abstract class DynamicIndexAbstractSelfTest extends AbstractSchemaSelfTest {
    /** Attribute to filter node out of cache data nodes. */
    private static final String ATTR_FILTERED = "FILTERED";

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
        node().getOrCreateCache(caseSensitiveCacheConfiguration());

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME);
        assertNoIndex(CACHE_NAME_SENSITIVE, TBL_NAME, IDX_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        node().destroyCache(CACHE_NAME);
        node().destroyCache(CACHE_NAME_SENSITIVE);

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
        final QueryIndex idx = index(IDX_NAME, field(FIELD_NAME_1));

        queryProcessor(node()).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false).get();
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME, field(FIELD_NAME_1));

        assertSchemaException(new RunnableX() {
            @Override public void run() throws Exception {
                queryProcessor(node()).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false).get();
            }
        }, SchemaOperationException.CODE_INDEX_EXISTS);

        queryProcessor(node()).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, true).get();
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME, field(FIELD_NAME_1));
    }

    /**
     * Test create when cache doesn't exist.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoCache() throws Exception {
        final QueryIndex idx = index(IDX_NAME, field(FIELD_NAME_1));

        assertSchemaException(new RunnableX() {
            @Override public void run() throws Exception {
                queryProcessor(node()).dynamicIndexCreate(randomString(), TBL_NAME, idx, false).get();
            }
        }, SchemaOperationException.CODE_CACHE_NOT_FOUND);

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME);
    }

    /**
     * Test create when table doesn't exist.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoTable() throws Exception {
        final QueryIndex idx = index(IDX_NAME, field(FIELD_NAME_1));

        assertSchemaException(new RunnableX() {
            @Override public void run() throws Exception {
                queryProcessor(node()).dynamicIndexCreate(CACHE_NAME, randomString(), idx, false).get();
            }
        }, SchemaOperationException.CODE_TABLE_NOT_FOUND);

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME);
    }

    /**
     * Test create when table doesn't exist.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoColumn() throws Exception {
        final QueryIndex idx = index(IDX_NAME, field(randomString()));

        assertSchemaException(new RunnableX() {
            @Override public void run() throws Exception {
                queryProcessor(node()).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false).get();
            }
        }, SchemaOperationException.CODE_COLUMN_NOT_FOUND);

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME);
    }

    /**
     * Test index creation on aliased column.
     *
     * @throws Exception If failed.
     */
    public void testCreateColumnWithAlias() throws Exception {
        assertSchemaException(new RunnableX() {
            @Override public void run() throws Exception {
                QueryIndex idx = index(IDX_NAME, field(FIELD_NAME_2));

                queryProcessor(node()).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false).get();
            }
        }, SchemaOperationException.CODE_COLUMN_NOT_FOUND);

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME);

        QueryIndex idx = index(IDX_NAME, field(alias(FIELD_NAME_2)));

        queryProcessor(node()).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false).get();
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME, field(alias(FIELD_NAME_2)));
    }

    /**
     * Test simple index create with schema case sensitivity considered.
     *
     * @throws Exception If failed.
     */
    // TODO: What is that?
    public void testCreateCaseSensitive() throws Exception {
        QueryIndex idx = index(IDX_NAME, field("Id"), field(FIELD_NAME_1), field("id", true));

        queryProcessor(node()).dynamicIndexCreate(CACHE_NAME_SENSITIVE, TBL_NAME, idx, false).get();
        assertIndex(CACHE_NAME_SENSITIVE, TBL_NAME, IDX_NAME, field("Id"), field(FIELD_NAME_1), field("id", true));

        queryProcessor(node()).dynamicIndexCreate(CACHE_NAME_SENSITIVE, TBL_NAME, idx, true).get();
        assertIndex(CACHE_NAME_SENSITIVE, TBL_NAME, IDX_NAME, field("Id"), field(FIELD_NAME_1), field("id", true));
    }

    /**
     * Test simple index drop.
     *
     * @throws Exception If failed.
     */
    public void testDrop() throws Exception {
        QueryIndex idx = index(IDX_NAME, field(FIELD_NAME_1));

        queryProcessor(node()).dynamicIndexCreate(CACHE_NAME, TBL_NAME, idx, false).get();
        assertIndex(CACHE_NAME, TBL_NAME, IDX_NAME, field(FIELD_NAME_1));

        queryProcessor(node()).dynamicIndexDrop(CACHE_NAME, IDX_NAME, false).get();
        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME);
    }

    /**
     * Test drop when there is no index.
     *
     * @throws Exception If failed.
     */
    public void testDropNoIndex() throws Exception {
        assertSchemaException(new RunnableX() {
            @Override public void run() throws Exception {
                queryProcessor(node()).dynamicIndexDrop(CACHE_NAME, IDX_NAME, false).get();
            }
        }, SchemaOperationException.CODE_INDEX_NOT_FOUND);

        queryProcessor(node()).dynamicIndexDrop(CACHE_NAME, IDX_NAME, true).get();
        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME);
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

        assertNoIndex(CACHE_NAME, TBL_NAME, IDX_NAME);
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
        return Arrays.asList(serverConfiguration(0), serverConfiguration(1));
    }

    /**
     * Create server configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    protected IgniteConfiguration serverConfiguration(int idx) throws Exception {
        return serverConfiguration(idx, false);
    }

    /**
     * Create server configuration.
     *
     * @param idx Index.
     * @param filter Whether to filter the node out of cache.
     * @return Configuration.
     * @throws Exception If failed.
     */
    protected IgniteConfiguration serverConfiguration(int idx, boolean filter) throws Exception {
        IgniteConfiguration cfg = commonConfiguration(idx);

        if (filter)
            cfg.setUserAttributes(Collections.singletonMap(ATTR_FILTERED, true));

        return cfg;
    }

    /**
     * Create client configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    protected IgniteConfiguration clientConfiguration(int idx) throws Exception {
        return commonConfiguration(idx).setClientMode(true);
    }

    /**
     * Create common node configuration.
     *
     * @param idx Index.
     * @return Configuration.
     * @throws Exception If failed.
     */
    protected IgniteConfiguration commonConfiguration(int idx) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(getTestIgniteInstanceName(idx));

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi() {
            @Override public void setListener(@Nullable DiscoverySpiListener lsnr) {
                super.setListener(GridTestUtils.DiscoverySpiListenerWrapper.wrap(lsnr, new Hook()));
            }
        };

        cfg.setDiscoverySpi(discoSpi);

        cfg.setMarshaller(new BinaryMarshaller());

        return optimize(cfg);
    }

    /**
     * Generic discovery hook.
     */
    private static class Hook extends GridTestUtils.DiscoveryHook {
        @Override public void handleDiscoveryMessage(DiscoverySpiCustomMessage msg) {
            if (msg != null)
                System.out.println("DISCO: " + msg);
        }
    }

    /**
     * @return Default cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration ccfg = new CacheConfiguration().setName(CACHE_NAME);

        QueryEntity entity = new QueryEntity();

        entity.setKeyType(KeyClass.class.getName());
        entity.setValueType(ValueClass.class.getName());

        entity.addQueryField("id", Long.class.getName(), null);
        entity.addQueryField(FIELD_NAME_1, String.class.getName(), null);
        entity.addQueryField(FIELD_NAME_2, String.class.getName(), null);

        entity.setKeyFields(Collections.singleton("id"));

        entity.setAliases(Collections.singletonMap(FIELD_NAME_2, alias(FIELD_NAME_2)));

        ccfg.setQueryEntities(Collections.singletonList(entity));

        return ccfg;
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
     * Ensure that schema exception is thrown.
     *
     * @param r Runnable.
     * @param expCode Error code.
     */
    private static void assertSchemaException(RunnableX r, int expCode) {
        try {
            r.run();
        }
        catch (SchemaOperationException e) {
            assertEquals("Unexpected error code [expected=" + expCode + ", actual=" + e.code() + ']',
                expCode, e.code());

            return;
        }
        catch (Exception e) {
            fail("Unexpected exception: " + e);
        }

        fail(SchemaOperationException.class.getSimpleName() +  " is not thrown.");
    }

    /**
     * @return Random string.
     */
    private static String randomString() {
        return UUID.randomUUID().toString();
    }

    /**
     * Runnable which can throw checked exceptions.
     */
    private interface RunnableX {
        /**
         * Do run.
         *
         * @throws Exception If failed.
         */
        public void run() throws Exception;
    }
}
