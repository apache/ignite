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
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.Person;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointListener;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.QueryIndexDescriptorImpl;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCachePartitionWorker;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitor;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorImpl;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Class for testing the resumption of index creation.
 */
public class ResumeCreateIndexTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        IndexesRebuildTaskEx.clean(getTestIgniteInstanceName());

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        IndexesRebuildTaskEx.clean(getTestIgniteInstanceName());

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
//            .setGridLogger(new ListeningTestLogger(log))
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
    public void test0() throws Exception {
        setRootLoggerDebugLevel();

        GridQueryProcessor.idxCls = IgniteH2IndexingEx.class;

        IgniteEx n = startGrid(0);
        n.cluster().state(ClusterState.ACTIVE);

        for (int i = 0; i < 100_000; i++)
            n.cache(DEFAULT_CACHE_NAME).put(i, new Person(i, "name_" + i));

        log.warning("kirill 0");

        SchemaIndexCachePartitionWorker.BATCH_SIZE = 1;

        GridFutureAdapter<Void> startIdxFut = new GridFutureAdapter<>();
        GridFutureAdapter<Void> finishIdxFut = new GridFutureAdapter<>();
        AtomicBoolean sleep = new AtomicBoolean(true);

        IgniteH2IndexingEx.consumers.put("IDX0", row -> {
            startIdxFut.onDone();

            finishIdxFut.get(getTestTimeout());

            if (sleep.get())
                doSleep(10);
        });

        IgniteEx n0 = n;
        IgniteInternalFuture<List<List<?>>> createIdxFut = runAsync(
            () -> n0.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("CREATE INDEX IDX0 ON Person(name)")).getAll());

        startIdxFut.get(getTestTimeout());

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
        finishIdxFut.onDone();

        enableCpFut.get(getTestTimeout());

        sleep.set(false);
        createIdxFut.get(getTestTimeout());

        log.warning("kirill 0");

        SqlFieldsQuery select = new SqlFieldsQuery("SELECT * FROM Person where name LIKE 'name_%';");
        log.warning("kirill select.size=" + n.cache(DEFAULT_CACHE_NAME).query(select).getAll().size());

        stopAllGrids();

        n = startGrid(0);
        n.cluster().state(ClusterState.ACTIVE);

        IgniteInternalFuture<?> rebIdxFut = indexRebuildFuture(n, CU.cacheId(DEFAULT_CACHE_NAME));

        if (rebIdxFut != null)
            rebIdxFut.get(getTestTimeout());

        log.warning("kirill select.size=" + n.cache(DEFAULT_CACHE_NAME).query(select).getAll().size());
    }

    private static class IgniteH2IndexingEx extends IgniteH2Indexing {
        static final Map<String, IgniteThrowableConsumer<CacheDataRow>> consumers = new ConcurrentHashMap<>();

        @Override public void dynamicIndexCreate(
            String schemaName,
            String tblName,
            QueryIndexDescriptorImpl idxDesc,
            boolean ifNotExists,
            SchemaIndexCacheVisitor cacheVisitor
        ) throws IgniteCheckedException {
            SchemaIndexCacheVisitor cacheVisitor0 = clo -> cacheVisitor.visit(row -> {
                consumers.getOrDefault(idxDesc.name(), row1 -> {}).accept(row);

                clo.apply(row);
            });

            super.dynamicIndexCreate(schemaName, tblName, idxDesc, ifNotExists, cacheVisitor0);
        }
    }
}
