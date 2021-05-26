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
import java.util.Objects;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.junit.Test;

/**
 * Cluster snapshot restore tests verifying SQL and indexing.
 */
public class IgniteClusterSnapshotRestoreWithIndexingTest extends IgniteClusterSnapshotRestoreBaseTest {
    /** Type name used for binary and SQL. */
    private static final String TYPE_NAME = IndexedObject.class.getName();

    /** Number of cache keys to pre-create at node start. */
    private static final int CACHE_KEYS_RANGE = 10_000;

    /** Cache value builder. */
    private Function<Integer, Object> valBuilder = new BinaryValueBuilder(TYPE_NAME);

    /** {@inheritDoc} */
    @Override protected <K, V> CacheConfiguration<K, V> txCacheConfig(CacheConfiguration<K, V> ccfg) {
        return super.txCacheConfig(ccfg).setSqlIndexMaxInlineSize(255).setSqlSchema("PUBLIC")
            .setQueryEntities(Collections.singletonList(new QueryEntity()
                .setKeyType(Integer.class.getName())
                .setValueType(TYPE_NAME)
                .setFields(new LinkedHashMap<>(F.asMap("id", Integer.class.getName(), "name", String.class.getName())))
                .setIndexes(Collections.singletonList(new QueryIndex("id")))));
    }

    /** {@inheritDoc} */
    @Override protected Function<Integer, Object> valueBuilder() {
        return valBuilder;
    }

    /** @throws Exception If failed. */
    @Test
    public void testBasicClusterSnapshotRestore() throws Exception {
        valBuilder = new IndexedValueBuilder();

        IgniteEx client = startGridsWithSnapshot(2, CACHE_KEYS_RANGE, true);

        grid(0).snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(DEFAULT_CACHE_NAME)).get(TIMEOUT);

        assertCacheKeys(client.cache(DEFAULT_CACHE_NAME), CACHE_KEYS_RANGE);
    }

    /** @throws Exception If failed. */
    @Test
    public void testBasicClusterSnapshotRestoreWithMetadata() throws Exception {
        IgniteEx ignite = startGridsWithSnapshot(2, CACHE_KEYS_RANGE);

        // Remove metadata.
        int typeId = ignite.context().cacheObjects().typeId(TYPE_NAME);

        ignite.context().cacheObjects().removeType(typeId);

        forceCheckpoint();

        ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, Collections.singleton(DEFAULT_CACHE_NAME)).get(TIMEOUT);

        assertCacheKeys(ignite.cache(DEFAULT_CACHE_NAME).withKeepBinary(), CACHE_KEYS_RANGE);

        for (Ignite grid : G.allGrids())
            assertNotNull(((IgniteEx)grid).context().cacheObjects().metadata(typeId));
    }

    /** @throws Exception If failed. */
    @Test
    public void testClusterSnapshotRestoreOnBiggerTopology() throws Exception {
        int nodesCnt = 4;

        startGridsWithCache(nodesCnt - 2, CACHE_KEYS_RANGE, valBuilder, dfltCacheCfg);

        grid(0).snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        startGrid(nodesCnt - 2);

        IgniteEx ignite = startGrid(nodesCnt - 1);

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

        assertCacheKeys(ignite.cache(DEFAULT_CACHE_NAME).withKeepBinary(), CACHE_KEYS_RANGE);
    }

    /** {@inheritDoc} */
    @Override protected void assertCacheKeys(IgniteCache<Object, Object> cache, int keysCnt) {
        super.assertCacheKeys(cache, keysCnt);

        String tblName = new BinaryBasicNameMapper(true).typeName(TYPE_NAME);

        for (Ignite grid : G.allGrids()) {
            GridKernalContext ctx = ((IgniteEx)grid).context();

            String nodeId = ctx.localNodeId().toString();

            assertTrue("nodeId=" + nodeId, grid.cache(cache.getName()).indexReadyFuture().isDone());

            // Make sure no index rebuild happened.
            assertEquals("nodeId=" + nodeId,
                0, ctx.cache().cache(cache.getName()).context().cache().metrics0().getIndexRebuildKeysProcessed());

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
