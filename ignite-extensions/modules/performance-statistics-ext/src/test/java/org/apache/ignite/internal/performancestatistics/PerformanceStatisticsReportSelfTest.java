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

package org.apache.ignite.internal.performancestatistics;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.IndexQuery;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.gt;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.waitForStatisticsEnabled;
import static org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsWriter.PERF_STAT_DIR;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests the performance statistics report.
 */
public class PerformanceStatisticsReportSelfTest {
    /** @throws Exception If failed. */
    @Test
    public void testCreateReport() throws Exception {
        try (
            Ignite srv = Ignition.start(new IgniteConfiguration().setIgniteInstanceName("srv"));

            IgniteEx client = (IgniteEx)Ignition.start(new IgniteConfiguration()
                .setIgniteInstanceName("client")
                .setClientMode(true))
        ) {
            client.context().performanceStatistics().startCollectStatistics();

            IgniteCache<Object, Object> cache = client.createCache(new CacheConfiguration<>("cache")
                .setQueryEntities(F.asList(new QueryEntity()
                    .setKeyType(Integer.class.getName())
                    .setValueType(Integer.class.getName()))));

            cache.put(0, 0);
            cache.put(1, 1);
            cache.get(1);
            cache.remove(1);
            cache.putAll(Collections.singletonMap(2, 2));
            cache.getAll(Collections.singleton(2));
            cache.removeAll(Collections.singleton(2));
            cache.getAndPut(3, 3);
            cache.getAndRemove(3);

            client.compute().run(() -> {
                // No-op.
            });

            assertThrowsWithCause(() -> client.compute().execute(new TaskWithoutJobs(), null), IgniteException.class);

            IgniteCache<Object, Object> txCache = client.createCache(new CacheConfiguration<>("txCache")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

            try (Transaction tx = client.transactions().txStart()) {
                txCache.put(1, 1);

                tx.commit();
            }

            try (Transaction tx = client.transactions().txStart()) {
                txCache.put(2, 2);

                tx.rollback();
            }

            cache.query(new ScanQuery<>((key, val) -> true)).getAll();

            cache.query(new SqlFieldsQuery("select * from sys.tables").setEnforceJoinOrder(true)).getAll();

            cache.query(new SqlFieldsQuery("select sum(_VAL) from \"cache\".Integer")).getAll();

            cache.query(new IndexQuery<>(Integer.class).setCriteria(gt("_KEY", 0))).getAll();

            client.context().performanceStatistics().stopCollectStatistics();

            waitForStatisticsEnabled(false);

            File prfDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), PERF_STAT_DIR, false);

            assertTrue(prfDir.exists());

            PerformanceStatisticsReportBuilder.main(prfDir.getAbsolutePath());

            File[] reportDir = prfDir.listFiles((dir, name) -> name.startsWith("report"));

            assertEquals(1, reportDir.length);

            File report = reportDir[0];

            File idx = new File(report.getAbsolutePath() + File.separatorChar + "index.html");
            File dataDir = new File(report.getAbsolutePath() + File.separatorChar + "data");
            File dataJs = new File(dataDir.getAbsolutePath() + File.separatorChar + "data.json.js");

            assertTrue(idx.exists());
            assertTrue(dataDir.exists());
            assertTrue(dataJs.exists());
        }
        finally {
            U.delete(new File(U.defaultWorkDirectory()));
        }
    }

    /** */
    private static class TaskWithoutJobs extends ComputeTaskAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(
            List subgrid,
            @Nullable Object arg
        ) throws IgniteException {
            return Collections.emptyMap();
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object reduce(List list) throws IgniteException {
            return null;
        }
    }
}
