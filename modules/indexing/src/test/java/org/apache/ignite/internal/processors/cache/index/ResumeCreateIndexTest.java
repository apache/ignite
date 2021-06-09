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

import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.Person;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.index.IndexingTestUtils.SlowdownBuildIndexConsumer;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.QueryIndexKey;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_INDEX_REBUILD_BATCH_SIZE;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

@WithSystemProperty(key = IGNITE_INDEX_REBUILD_BATCH_SIZE, value = "1")
public class ResumeCreateIndexTest extends AbstractRebuildIndexTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                cacheCfg(DEFAULT_CACHE_NAME, null).setAffinity(new RendezvousAffinityFunction(false, 1))
            );
    }

    @Test
    public void test0() throws Exception {
        IgniteH2IndexingEx.prepareBeforeNodeStart();

        IgniteEx n = startGrid(0);

        String cacheName = DEFAULT_CACHE_NAME;
        populate(n.cache(cacheName), 100_000);

        String idxName = "IDX0";
        SlowdownBuildIndexConsumer slowdownIdxCreateConsumer = addSlowdownIdxCreateConsumer(n, idxName, 10);

        IgniteInternalFuture<List<List<?>>> createIdxFut = createIdxAsync(n.cache(cacheName), idxName);

        slowdownIdxCreateConsumer.startBuildIdxFut.get(getTestTimeout());

        String reason = getTestIgniteInstanceName();
        IgniteInternalFuture<Void> awaitBeforeCpBeginFut = awaitBeforeCheckpointBeginAsync(n, reason);
        IgniteInternalFuture<Void> disableCpFut = disableCheckpointsAsync(n, reason);

        awaitBeforeCpBeginFut.get(getTestTimeout());
        slowdownIdxCreateConsumer.finishBuildIdxFut.onDone();

        disableCpFut.get(getTestTimeout());
        slowdownIdxCreateConsumer.sleepTime.set(0);

        createIdxFut.get(getTestTimeout());

        stopGrid(0);

        n = startGrid(0);

        assertTrue(allIndexes(n).containsKey(new QueryIndexKey(cacheName, idxName)));

        IgniteInternalFuture<?> rebIdxFut = indexRebuildFuture(n, CU.cacheId(cacheName));

        if (rebIdxFut != null)
            rebIdxFut.get(getTestTimeout());

        assertEquals(100_000, selectPersonByName(n.cache(cacheName)).size());
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
            String sql = "CREATE INDEX " + idxName + " ON Person(name)";

            return cache.query(new SqlFieldsQuery(sql)).getAll();
        });
    }

    /**
     * Disable checkpoints asynchronously.
     *
     * @param n Node.
     * @param reason Reason for checkpoint wakeup if it would be required.
     * @return Disable checkpoints future.
     */
    private IgniteInternalFuture<Void> disableCheckpointsAsync(IgniteEx n, String reason) {
        return runAsync(() -> {
            forceCheckpoint(F.asList(n), reason);

            dbMgr(n).enableCheckpoints(false).get(getTestTimeout());

            return null;
        });
    }

    /**
     * Waiting for a {@link CheckpointListener#beforeCheckpointBegin} asynchronously
     * for a checkpoint for a specific reason.
     *
     * @param n Node.
     * @param reason Checkpoint reason.
     * @return Future for waiting for the {@link CheckpointListener#beforeCheckpointBegin}.
     */
    private IgniteInternalFuture<Void> awaitBeforeCheckpointBeginAsync(IgniteEx n, String reason) {
        GridFutureAdapter<Void> fut = new GridFutureAdapter<>();

        dbMgr(n).addCheckpointListener(new CheckpointListener() {
            /** {@inheritDoc} */
            @Override public void onMarkCheckpointBegin(Context ctx) throws IgniteCheckedException {
                // No-op.
            }

            /** {@inheritDoc} */
            @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {
                // No-op.
            }

            /** {@inheritDoc} */
            @Override public void beforeCheckpointBegin(Context ctx) throws IgniteCheckedException {
                if (reason.equals(ctx.progress().reason()))
                    fut.onDone();
            }
        });

        return fut;
    }

    /**
     * Getting {@code GridQueryProcessor#idxs}.
     *
     * @param n Node.
     * @return All indexes.
     */
    private Map<QueryIndexKey, QueryIndexDescriptorImpl> allIndexes(IgniteEx n) {
        return getFieldValue(n.context().query(), "idxs");
    }

    /**
     * Selection of all {@link Person} by name.
     * SQL: SELECT * FROM Person where name LIKE 'name_%';
     *
     * @param cache Cache.
     * @return List containing all query results.
     */
    private List<List<?>> selectPersonByName(IgniteCache<Integer, Person> cache) {
        return cache.query(new SqlFieldsQuery("SELECT * FROM Person where name LIKE 'name_%';")).getAll();
    }
}
