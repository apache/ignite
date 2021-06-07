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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.Person;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.index.IndexingTestUtils.SlowDownBuildIndexConsumer;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_INDEX_REBUILD_BATCH_SIZE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.cache.index.IgniteH2IndexingEx.addIdxCreateRowConsumer;
import static org.apache.ignite.internal.processors.cache.index.IndexingTestUtils.nodeName;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Class for testing the resumption of index creation.
 */
public class ResumeCreateIndexTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        IgniteH2IndexingEx.clean(getTestIgniteInstanceName());

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        IgniteH2IndexingEx.clean(getTestIgniteInstanceName());

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConsistentId(igniteInstanceName)
            .setFailureHandler(new StopNodeFailureHandler())
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            ).setCacheConfiguration(
                new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                    .setIndexedTypes(Integer.class, Person.class)
                    .setAffinity(new RendezvousAffinityFunction(false, 1))
            );
    }

    @Test
    @WithSystemProperty(key = IGNITE_INDEX_REBUILD_BATCH_SIZE, value = "1")
    public void test0() throws Exception {
        IgniteH2IndexingEx.prepareBeforeNodeStart();

        IgniteEx n = startGrid(0);
        n.cluster().state(ACTIVE);

        for (int i = 0; i < 100_000; i++)
            n.cache(DEFAULT_CACHE_NAME).put(i, new Person(i, "name_" + i));

        log.warning("kirill 0");

        SlowDownBuildIndexConsumer consumer = new SlowDownBuildIndexConsumer(getTestTimeout(), 10);
        addIdxCreateRowConsumer(nodeName(n), "IDX0", consumer);

        IgniteEx n0 = n;
        IgniteInternalFuture<List<List<?>>> createIdxFut = runAsync(
            () -> n0.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("CREATE INDEX IDX0 ON Person(name)")).getAll());

        consumer.startFut.get(getTestTimeout());

        GridFutureAdapter<Void> startCpFut = new GridFutureAdapter<>();

        dbMgr(n).addCheckpointListener(new CheckpointListener() {
            @Override public void onMarkCheckpointBegin(Context ctx) throws IgniteCheckedException {

            }

            @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {

            }

            @Override public void beforeCheckpointBegin(Context ctx) throws IgniteCheckedException {
                if ("test".equals(ctx.progress().reason()))
                    startCpFut.onDone();
            }
        });

        IgniteInternalFuture<Object> enableCpFut = runAsync(() -> {
            forceCheckpoint();

            dbMgr(n0).enableCheckpoints(false).get(getTestTimeout());

            return null;
        });

        startCpFut.get(getTestTimeout());
        consumer.finishFut.onDone();

        enableCpFut.get(getTestTimeout());

        consumer.sleepTime.set(0);
        createIdxFut.get(getTestTimeout());

        log.warning("kirill 0");

        SqlFieldsQuery select = new SqlFieldsQuery("SELECT * FROM Person where name LIKE 'name_%';");
        log.warning("kirill select.size=" + n.cache(DEFAULT_CACHE_NAME).query(select).getAll().size());

        stopAllGrids();

        n = startGrid(0);
        n.cluster().state(ACTIVE);

        IgniteInternalFuture<?> rebIdxFut = indexRebuildFuture(n, CU.cacheId(DEFAULT_CACHE_NAME));

        if (rebIdxFut != null)
            rebIdxFut.get(getTestTimeout());

        log.warning("kirill select.size=" + n.cache(DEFAULT_CACHE_NAME).query(select).getAll().size());
    }
}
