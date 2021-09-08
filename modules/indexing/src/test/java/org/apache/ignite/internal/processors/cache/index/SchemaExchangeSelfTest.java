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

import java.util.Collections;
import java.util.Map;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.QueryTypeDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteClientReconnectAbstractTest.reconnectClientNode;

/**
 * Tests for schema exchange between nodes.
 */
public class SchemaExchangeSelfTest extends AbstractSchemaSelfTest {
    /** Node on which filter should be applied (if any). */
    private static String filterNodeName;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        filterNodeName = null;

        super.afterTest();
    }

    /**
     * Test propagation of empty query schema for static cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEmptyStatic() throws Exception {
        checkEmpty(false);
    }

    /**
     * Test propagation of empty query schema for dynamic cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEmptyDynamic() throws Exception {
        checkEmpty(true);
    }

    /**
     * Check empty metadata propagation.
     *
     * @param dynamic Dynamic start flag.
     * @throws Exception If failed.
     */
    private void checkEmpty(boolean dynamic) throws Exception {
        IgniteEx node1;

        if (dynamic) {
            node1 = startNoCache(1);

            node1.getOrCreateCache(cacheConfiguration());
        }
        else
            node1 = start(1);

        assertTypes(node1);

        IgniteEx node2 = start(2, KeyClass.class, ValueClass.class);

        assertTypes(node1);
        assertTypes(node2);

        IgniteEx node3 = start(3, KeyClass.class, ValueClass.class, KeyClass2.class, ValueClass2.class);

        assertTypes(node1);
        assertTypes(node2);
        assertTypes(node3);
    }

    /**
     * Test propagation of non-empty query schema for static cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNonEmptyStatic() throws Exception {
        checkNonEmpty(false);
    }

    /**
     * Test propagation of non-empty query schema for dynamic cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNonEmptyDynamic() throws Exception {
        checkNonEmpty(true);
    }

    /**
     * Check тщт-empty metadata propagation.
     *
     * @param dynamic Dynamic start flag.
     * @throws Exception If failed.
     */
    private void checkNonEmpty(boolean dynamic) throws Exception {
        IgniteEx node1;

        if (dynamic) {
            node1 = startNoCache(1);

            node1.getOrCreateCache(cacheConfiguration(KeyClass.class, ValueClass.class));
        }
        else
            node1 = start(1, KeyClass.class, ValueClass.class);

        assertTypes(node1, ValueClass.class);

        IgniteEx node2 = start(2);

        assertTypes(node1, ValueClass.class);
        assertTypes(node2, ValueClass.class);

        IgniteEx node3 = start(3, KeyClass.class, ValueClass.class, KeyClass2.class, ValueClass2.class);

        assertTypes(node1, ValueClass.class);
        assertTypes(node2, ValueClass.class);
        assertTypes(node3, ValueClass.class);
    }

    /**
     * Make sure that new metadata can be propagated after destroy.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDynamicRestarts() throws Exception {
        IgniteEx node1 = start(1, KeyClass.class, ValueClass.class);
        IgniteEx node2 = startNoCache(2);
        IgniteEx node3 = startClientNoCache(3);
        IgniteEx node4 = startClientNoCache(4);

        assertTypes(node1, ValueClass.class);
        assertTypes(node2, ValueClass.class);
        assertCacheStarted(CACHE_NAME, node1, node2);

        assertTypes(node3, ValueClass.class);
        assertCacheNotStarted(CACHE_NAME, node3);

        node3.cache(CACHE_NAME);
        assertCacheStarted(CACHE_NAME, node3);

        // Check restarts from the first node.
        destroySqlCache(node1);

        node1.getOrCreateCache(cacheConfiguration());

        assertTypes(node1);
        assertTypes(node2);
        assertTypes(node3);

        node1.destroyCache(CACHE_NAME);

        node1.getOrCreateCache(cacheConfiguration(KeyClass.class, ValueClass.class,
            KeyClass2.class, ValueClass2.class));

        assertTypes(node1, ValueClass.class, ValueClass2.class);
        assertTypes(node2, ValueClass.class, ValueClass2.class);
        assertTypes(node3, ValueClass.class, ValueClass2.class);

        assertCacheStarted(CACHE_NAME, node1, node2);

        assertCacheNotStarted(CACHE_NAME, node3);

        node3.cache(CACHE_NAME);
        assertCacheStarted(CACHE_NAME, node3);

        // Check restarts from the second node.
        node2.destroyCache(CACHE_NAME);

        node2.getOrCreateCache(cacheConfiguration());

        assertTypes(node1);
        assertTypes(node2);
        assertTypes(node3);

        node2.destroyCache(CACHE_NAME);

        node2.getOrCreateCache(cacheConfiguration(KeyClass.class, ValueClass.class,
            KeyClass2.class, ValueClass2.class));

        assertTypes(node1, ValueClass.class, ValueClass2.class);
        assertTypes(node2, ValueClass.class, ValueClass2.class);
        assertTypes(node3, ValueClass.class, ValueClass2.class);
        assertTypes(node4, ValueClass.class, ValueClass2.class);

        assertCacheStarted(CACHE_NAME, node1, node2);

        assertCacheNotStarted(CACHE_NAME, node3);

        node3.cache(CACHE_NAME);
        assertCacheStarted(CACHE_NAME, node3);

        assertCacheNotStarted(CACHE_NAME, node4);

        node4.cache(CACHE_NAME);
        assertCacheStarted(CACHE_NAME, node4);

        // Make sure that joining node observes correct state.
        assertTypes(start(5), ValueClass.class, ValueClass2.class);
        assertTypes(startNoCache(6), ValueClass.class, ValueClass2.class);

        assertTypes(startClient(7), ValueClass.class, ValueClass2.class);

        IgniteEx node8 = startClientNoCache(8);

        assertTypes(node8, ValueClass.class, ValueClass2.class);

        assertCacheNotStarted(CACHE_NAME, node8);

        node8.cache(CACHE_NAME);

        assertCacheStarted(CACHE_NAME, node8);
    }

    /**
     * Test client join for static cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientJoinStatic() throws Exception {
        checkClientJoin(false);
    }

    /**
     * Test client join for dynamic cache.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientJoinDynamic() throws Exception {
        checkClientJoin(true);
    }

    /**
     * Check client join.
     *
     * @throws Exception If failed.
     */
    private void checkClientJoin(boolean dynamic) throws Exception {
        IgniteEx node1;

        if (dynamic) {
            node1 = startNoCache(1);

            node1.getOrCreateCache(cacheConfiguration(KeyClass.class, ValueClass.class));
        }
        else
            node1 = start(1, KeyClass.class, ValueClass.class);

        IgniteEx node2 = startClient(2);

        assertTypes(node1, ValueClass.class);
        assertTypes(node2, ValueClass.class);

        IgniteEx node3 = startClient(3, KeyClass.class, ValueClass.class,
            KeyClass2.class, ValueClass2.class);

        assertTypes(node1, ValueClass.class);
        assertTypes(node2, ValueClass.class);
        assertTypes(node3, ValueClass.class);

        IgniteEx node4 = startClientNoCache(4);

        assertTypes(node1, ValueClass.class);
        assertTypes(node2, ValueClass.class);
        assertTypes(node3, ValueClass.class);

        assertTypes(node4, ValueClass.class);

        assertCacheStarted(CACHE_NAME, node1, node2, node3);

        assertCacheNotStarted(CACHE_NAME, node4);

        node4.cache(CACHE_NAME);

        assertCacheStarted(CACHE_NAME, node4);
    }

    /**
     * Test client cache start (static).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientCacheStartStatic() throws Exception {
        checkClientCacheStart(false);
    }

    /**
     * Test client cache start (dynamic).
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientCacheStartDynamic() throws Exception {
        checkClientCacheStart(true);
    }

    /**
     * Check client cache start.
     *
     * @throws Exception If failed.
     */
    private void checkClientCacheStart(boolean dynamic) throws Exception {
        IgniteEx node1 = startNoCache(1);

        IgniteEx node2;

        if (dynamic) {
            node2 = startClientNoCache(2);

            createSqlCache(node2, cacheConfiguration(KeyClass.class, ValueClass.class));
        }
        else
            node2 = startClient(2, KeyClass.class, ValueClass.class);

        assertTypes(node1, ValueClass.class);
        assertTypes(node2, ValueClass.class);

        IgniteEx node3 = start(3);
        IgniteEx node4 = start(4, KeyClass.class, ValueClass.class, KeyClass2.class, ValueClass2.class);
        IgniteEx node5 = startNoCache(5);

        IgniteEx node6 = startClient(6);
        IgniteEx node7 = startClient(7, KeyClass.class, ValueClass.class, KeyClass2.class, ValueClass2.class);
        IgniteEx node8 = startClientNoCache(8);

        assertCacheStarted(CACHE_NAME, node1, node2, node3, node4, node5, node6, node7);
        assertCacheNotStarted(CACHE_NAME, node8);
        assertTypes(node8, ValueClass.class);

        node8.cache(CACHE_NAME);

        assertCacheStarted(CACHE_NAME, node8);

        destroySqlCache(node2);

        node2.getOrCreateCache(
            cacheConfiguration(KeyClass.class, ValueClass.class, KeyClass2.class, ValueClass2.class));

        assertCacheStarted(CACHE_NAME, node1, node2, node3, node4, node5);
        assertCacheNotStarted(CACHE_NAME, node6, node7, node8);

        assertTypes(node6, ValueClass.class, ValueClass2.class);
        assertTypes(node7, ValueClass.class, ValueClass2.class);
        assertTypes(node8, ValueClass.class, ValueClass2.class);

        node6.cache(CACHE_NAME);
        node7.cache(CACHE_NAME);
        node8.cache(CACHE_NAME);

        assertCacheStarted(CACHE_NAME, node6, node7, node8);
    }

    /**
     * Test behavior when node filter is set.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNodeFilter() throws Exception {
        filterNodeName = getTestIgniteInstanceName(1);

        IgniteEx node1 = start(1, KeyClass.class, ValueClass.class);
        assertTypes(node1, ValueClass.class);

        IgniteEx node2 = start(2, KeyClass.class, ValueClass.class);
        assertTypes(node1, ValueClass.class);
        assertTypes(node2, ValueClass.class);

        IgniteEx node3 = startNoCache(3);
        assertTypes(node1, ValueClass.class);
        assertTypes(node2, ValueClass.class);
        assertTypes(node3, ValueClass.class);

        assertCacheStarted(CACHE_NAME, node1, node2);

        assertCacheNotStarted(CACHE_NAME, node3);

        node3.cache(CACHE_NAME);
        assertCacheStarted(CACHE_NAME, node1, node3);
    }

    /**
     * Test client reconnect after server restart accompanied by schema change.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testServerRestartWithNewTypes() throws Exception {
        IgniteEx node1 = start(1, KeyClass.class, ValueClass.class);
        assertTypes(node1, ValueClass.class);

        IgniteEx node2 = startClientNoCache(2);

        GridCacheContext<Object, Object> context0 = node2.context().cache().context().cacheContext(CU.cacheId(CACHE_NAME));
        node2.cache(CACHE_NAME);
        GridCacheContext<Object, Object> context = node2.context().cache().context().cacheContext(CU.cacheId(CACHE_NAME));
        GridCacheAdapter<Object, Object> entries = node2.context().cache().internalCache(CACHE_NAME);
        assertTrue(entries.active());

        node2.cache(CACHE_NAME);
        assertTypes(node2, ValueClass.class);

        stopGrid(1);

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return grid(2).context().clientDisconnected();
            }
        }, 10_000L));

        IgniteFuture reconnFut = null;

        try {
            node2.cache(CACHE_NAME);

            fail();
        }
        catch (IgniteClientDisconnectedException e) {
            reconnFut = e.reconnectFuture();
        }

        node1 = start(1, KeyClass.class, ValueClass.class, KeyClass2.class, ValueClass2.class);
        assertTypes(node1, ValueClass.class, ValueClass2.class);

        assertCacheStarted(CACHE_NAME, node1);

        reconnFut.get();

        assertCacheNotStarted(CACHE_NAME, node2);

        node2.cache(CACHE_NAME);

        assertCacheStarted(CACHE_NAME, node2);

        assertTypes(node2, ValueClass.class, ValueClass2.class);
    }

    /**
     * Test client reconnect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClientReconnect() throws Exception {
        final IgniteEx node1 = start(1, KeyClass.class, ValueClass.class);
        assertTypes(node1, ValueClass.class);

        final IgniteEx node2 = startClientNoCache(2);
        assertTypes(node2, ValueClass.class);
        assertCacheNotStarted(CACHE_NAME, node2);

        node2.cache(CACHE_NAME);
        assertCacheStarted(CACHE_NAME, node2);

        reconnectClientNode(log, node2, node1, new Runnable() {
            @Override public void run() {
                assertTrue(node2.context().clientDisconnected());

                final QueryIndex idx = index(IDX_NAME_1, field(FIELD_NAME_1_ESCAPED));

                try {
                    dynamicIndexCreate(node1, CACHE_NAME, TBL_NAME, idx, false, 0);
                }
                catch (Exception e) {
                    throw new IgniteException(e);
                }
            }
        });

        assertIndex(CACHE_NAME,
            QueryUtils.normalizeObjectName(TBL_NAME, true),
            QueryUtils.normalizeObjectName(IDX_NAME_1, false), QueryIndex.DFLT_INLINE_SIZE,
            field(QueryUtils.normalizeObjectName(FIELD_NAME_1_ESCAPED, false)));
    }

    /**
     * Check is cache started on the given node or not.
     *
     * @param cacheName Cache name.
     * @param node Node to check cache.
     * @return {@code true} in case cache is started.
     */
    private boolean isCacheStarted(String cacheName, IgniteEx node) {
        GridCacheContext cacheCtx = node.context().cache().context().cacheContext(CU.cacheId(cacheName));

        return cacheCtx != null;
    }

    /**
     * Check is cache started on the given nodes.
     *
     * @param nodes Node to check cache.
     * @param cacheName Cache name.
     * @throws AssertionError If failed.
     */
    private void assertCacheStarted(String cacheName, IgniteEx... nodes) throws AssertionError {
        for (IgniteEx node : nodes)
            assertTrue(isCacheStarted(cacheName, node));
    }

    /**
     * Check is cache not started on the given nodes.
     *
     * @param nodes Node to check cache.
     * @param cacheName Cache name.
     * @throws AssertionError If failed.
     */
    private void assertCacheNotStarted(String cacheName, IgniteEx... nodes) throws AssertionError {
        for (IgniteEx node : nodes)
            assertFalse(isCacheStarted(cacheName, node));
    }

    /**
     * Ensure that only provided types exists for the given cache.
     *
     * @param node Node.
     * @param clss Classes.
     */
    private static void assertTypes(IgniteEx node, Class... clss) {
        Map<String, QueryTypeDescriptorImpl> types = types(node, CACHE_NAME);

        if (F.isEmpty(clss))
            assertTrue(types.isEmpty());
        else {
            assertEquals(clss.length, types.size());

            for (Class cls : clss) {
                String tblName = tableName(cls);

                assertTrue(types.containsKey(tblName));
            }
        }
    }

    /**
     * Start node with the given cache configuration.
     *
     * @param clss Key-value classes.
     * @return Node.
     */
    private IgniteEx start(int idx, Class... clss) throws Exception {
        return start(idx, false, clss);
    }

    /**
     * Start client node with the given cache configuration.
     *
     * @param clss Key-value classes.
     * @return Node.
     */
    private IgniteEx startClient(int idx, Class... clss) throws Exception {
        return start(idx, true, clss);
    }

    /**
     * Start node with the given cache configuration.
     *
     * @param idx Index.
     * @param client Client flag.
     * @param clss Key-value classes.
     * @return Node.
     */
    private IgniteEx start(int idx, boolean client, Class... clss) throws Exception {
        String name = getTestIgniteInstanceName(idx);

        IgniteConfiguration cfg = getConfiguration(name);

        cfg.setClientMode(client);
        cfg.setLocalHost("127.0.0.1");

        if (filterNodeName != null && F.eq(name, filterNodeName))
            cfg.setUserAttributes(Collections.singletonMap("AFF_NODE", true));

        IgniteEx res = (IgniteEx)Ignition.start(cfg);

        createSqlCache(res, cacheConfiguration(clss));

        return res;
    }

    /**
     * Start node without cache.
     *
     * @param idx Index.
     * @return Node.
     * @throws Exception If failed.
     */
    private IgniteEx startNoCache(int idx) throws Exception {
        return startNoCache(idx, false);
    }

    /**
     * Start client node without cache.
     *
     * @param idx Index.
     * @return Node.
     * @throws Exception If failed.
     */
    private IgniteEx startClientNoCache(int idx) throws Exception {
        return startNoCache(idx, true);
    }

    /**
     * Start node without cache.
     *
     * @param idx Index.
     * @param client Client mode flag.
     * @return Node.
     * @throws Exception If failed.
     */
    private IgniteEx startNoCache(int idx, boolean client) throws Exception {
        String name = getTestIgniteInstanceName(idx);

        IgniteConfiguration cfg = getConfiguration(name);

        cfg.setClientMode(client);
        cfg.setLocalHost("127.0.0.1");

        return (IgniteEx)Ignition.start(cfg);
    }

    /**
     * Get cache configuration.
     *
     * @param clss QUery classes.
     * @return Configuration.
     */
    @SuppressWarnings("unchecked")
    private static CacheConfiguration cacheConfiguration(Class... clss) {
        CacheConfiguration ccfg = new CacheConfiguration().setName(CACHE_NAME).setIndexedTypes(clss);

        if (filterNodeName != null) {
            ccfg.setNodeFilter(new IgnitePredicate<ClusterNode>() {
                @Override public boolean apply(ClusterNode node) {
                    return node.attribute("AFF_NODE") != null;
                }
            });
        }

        return ccfg;
    }

    // TODO: Start/stop many nodes with static configs and dynamic start/stop.

    /**
     * Key class 2.
     */
    @SuppressWarnings("unused")
    private static class KeyClass2 {
        @QuerySqlField
        private String keyField2;
    }

    /**
     * Value class 2.
     */
    @SuppressWarnings("unused")
    private static class ValueClass2 {
        @QuerySqlField
        private String valField2;
    }
}
