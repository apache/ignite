/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.SnapshotEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.util.Optional.ofNullable;
import static org.apache.ignite.events.EventType.EVTS_CLUSTER_SNAPSHOT;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_SNAPSHOT_RESTORE_FINISHED;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED;

/**
 * Cluster snapshot restore tests verifying SQL and indexing.
 */
public class IgniteClusterSnapshotRestoreWithIndexingTest extends IgniteClusterSnapshotRestoreBaseTest {
    /** Type name used for binary and SQL. */
    private static final String TYPE_NAME = IndexedObject.class.getName();

    /** Number of cache keys to pre-create at node start. */
    private static final int CACHE_KEYS_RANGE = 10_000;

    /** {@inheritDoc} */
    @Override protected <K, V> CacheConfiguration<K, V> txCacheConfig(CacheConfiguration<K, V> ccfg) {
        return super.txCacheConfig(ccfg).setSqlIndexMaxInlineSize(255).setSqlSchema("PUBLIC")
            .setQueryEntities(Collections.singletonList(new QueryEntity()
                .setKeyType(Integer.class.getName())
                .setValueType(TYPE_NAME)
                .setFields(new LinkedHashMap<>(F.asMap("id", Integer.class.getName(), "name", String.class.getName())))
                .setIndexes(Collections.singletonList(new QueryIndex("id")))));
    }

    /** @throws Exception If failed. */
    @Test
    public void testBasicClusterSnapshotRestore() throws Exception {
        valBuilder = new IndexedValueBuilder();

        IgniteEx client = startGridsWithSnapshot(2, CACHE_KEYS_RANGE, true);

        grid(0).snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(DEFAULT_CACHE_NAME)).get(TIMEOUT);

        // Only primary mode leads to index rebuild on restore.
        // Must wait until index rebuild finish so subsequent checks will pass.
        if (onlyPrimary)
            awaitPartitionMapExchange();

        assertCacheKeys(client.cache(DEFAULT_CACHE_NAME), CACHE_KEYS_RANGE);
        assertRebuildIndexes(client.cache(DEFAULT_CACHE_NAME), onlyPrimary);

        waitForEvents(EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED, EVT_CLUSTER_SNAPSHOT_RESTORE_FINISHED);
    }

    /** @throws Exception If failed. */
    @Test
    public void testBasicClusterSnapshotRestoreWithMetadata() throws Exception {
        valBuilder = new BinaryValueBuilder(TYPE_NAME);

        IgniteEx ignite = startGridsWithSnapshot(2, CACHE_KEYS_RANGE);

        // Remove metadata.
        int typeId = ignite.context().cacheObjects().typeId(TYPE_NAME);

        ignite.context().cacheObjects().removeType(typeId);

        forceCheckpoint();

        ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(DEFAULT_CACHE_NAME)).get(TIMEOUT);

        if (onlyPrimary)
            awaitPartitionMapExchange();

        assertCacheKeys(ignite.cache(DEFAULT_CACHE_NAME).withKeepBinary(), CACHE_KEYS_RANGE);
        assertRebuildIndexes(ignite.cache(DEFAULT_CACHE_NAME).withKeepBinary(), onlyPrimary);

        for (Ignite grid : G.allGrids())
            assertNotNull(((IgniteEx)grid).context().cacheObjects().metadata(typeId));

        waitForEvents(EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED, EVT_CLUSTER_SNAPSHOT_RESTORE_FINISHED);
    }

    /** @throws Exception If failed. */
    @Test
    public void testClusterSnapshotRestoreOnBiggerTopology() throws Exception {
        valBuilder = new BinaryValueBuilder(TYPE_NAME);

        int nodesCnt = 4;

        startGridsWithCache(nodesCnt - 2, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        grid(0).snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        startGrid(nodesCnt - 2);

        IgniteEx ignite = startGrid(nodesCnt - 1);

        List<SnapshotEvent> evts = new CopyOnWriteArrayList<>();

        ignite.events().localListen(e -> e instanceof SnapshotEvent && evts.add((SnapshotEvent)e), EVTS_CLUSTER_SNAPSHOT);

        resetBaselineTopology();

        awaitPartitionMapExchange();

        ignite.cache(DEFAULT_CACHE_NAME).destroy();

        awaitPartitionMapExchange();

        // Remove metadata.
        int typeId = ignite.context().cacheObjects().typeId(TYPE_NAME);

        ignite.context().cacheObjects().removeType(typeId);

        forceCheckpoint();

        // Restore from an empty node.
        ignite.snapshot().restoreSnapshot(
            SNAPSHOT_NAME, Collections.singleton(DEFAULT_CACHE_NAME)).get(TIMEOUT);

        awaitPartitionMapExchange();

        for (Ignite g : G.allGrids())
            ofNullable(indexRebuildFuture((IgniteEx)g, CU.cacheId(DEFAULT_CACHE_NAME))).orElse(new GridFinishedFuture<>()).get(TIMEOUT);

        for (Ignite g : G.allGrids())
            assertCacheKeys(g.cache(DEFAULT_CACHE_NAME).withKeepBinary(), CACHE_KEYS_RANGE);

        GridTestUtils.waitForCondition(() -> evts.size() == 2, TIMEOUT);
        assertEquals(2, evts.size());

        SnapshotEvent startEvt = evts.get(0);

        assertEquals(SNAPSHOT_NAME, startEvt.snapshotName());
        assertTrue(startEvt.message().contains("caches=[" + DEFAULT_CACHE_NAME + ']'));
    }

    /** {@inheritDoc} */
    @Override protected void assertCacheKeys(IgniteCache<Object, Object> cache, int keysCnt) {
        super.assertCacheKeys(cache, keysCnt);

        String tblName = new BinaryBasicNameMapper(true).typeName(TYPE_NAME);

        for (Ignite grid : G.allGrids()) {
            GridKernalContext ctx = ((IgniteEx)grid).context();

            String nodeId = ctx.localNodeId().toString();

            GridQueryProcessor qry = ((IgniteEx)grid).context().query();

            // Make sure  SQL works fine.
            assertEquals("nodeId=" + nodeId, (long)keysCnt, qry.querySqlFields(new SqlFieldsQuery(
                "SELECT count(*) FROM " + tblName), true).getAll().get(0).get(0));

            // Make sure the index is in use.
            String explainPlan = (String)qry.querySqlFields(new SqlFieldsQuery(
                "explain SELECT * FROM " + tblName + " WHERE id < 10"), true).getAll().get(0).get(0);

            assertTrue("nodeId=" + nodeId + "\n" + explainPlan, explainPlan.contains("ID_ASC_IDX"));
        }
    }

    /**
     * @param cache Ignite cache.
     * @param rebuild Rebuild index happened.
     */
    private void assertRebuildIndexes(IgniteCache<Object, Object> cache, boolean rebuild) {
        for (Ignite grid : G.allGrids()) {
            GridKernalContext ctx = ((IgniteEx)grid).context();

            assertTrue("nodeId=" + ctx.localNodeId(), grid.cache(cache.getName()).indexReadyFuture().isDone());

            if (grid.configuration().isClientMode())
                continue;

            // Make sure no index rebuild happened.
            assertEquals("nodeId=" + ctx.localNodeId(),
                rebuild, ctx.cache().cache(cache.getName()).context().cache().metrics0()
                    .getIndexRebuildKeysProcessed() > 0);
        }
    }

    /** */
    private static class IndexedValueBuilder implements Function<Integer, Object> {
        /** {@inheritDoc} */
        @Override public Object apply(Integer key) {
            return new IndexedObject(key, "Person number #" + key);
        }
    }

    /** */
    private static class IndexedObject {
        /** Id. */
        @QuerySqlField(index = true)
        private final int id;

        /** Name. */
        @QuerySqlField
        private final String name;

        /**
         * @param id Id.
         */
        public IndexedObject(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            IndexedObject obj = (IndexedObject)o;

            return id == obj.id && Objects.equals(name, obj.name);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(name, id);
        }
    }
}
