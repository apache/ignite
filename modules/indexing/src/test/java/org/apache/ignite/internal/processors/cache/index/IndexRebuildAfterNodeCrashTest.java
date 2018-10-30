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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.Person;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;

public class IndexRebuildAfterNodeCrashTest extends GridCommonAbstractTest {

    private static final String CACHE_NAME = "foo";

    private static final long RECORDS_NUM = 500_000;

    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
                .setWalSegmentSize(2 * 1024 * 1024)
        );

        cfg.setConsistentId(igniteInstanceName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    public void testIndexRebuildAfterNodeCrashInTimeIndexRebuild() throws Exception {
        IgniteEx ignite = (IgniteEx)startGrids(2);

        ignite.cluster().active(true);

        IgniteCache<Integer, Person> cache = ignite.getOrCreateCache(
            new CacheConfiguration<Integer, Person>()
                .setName(CACHE_NAME)
                .setIndexedTypes(Integer.class, Person.class)
                .setBackups(0)
        );

        populateData(cache);

        checkSize(cache);

        Thread t = new Thread(() -> {
            log.info("Start create index!");

            long startTime = U.currentTimeMillis();

            cache.query(new SqlFieldsQuery("CREATE INDEX IDX_PERSON_NAME ON Person (name)")).getAll();

            long delta = U.currentTimeMillis() - startTime;

            log.info("Index created! time: " + delta + " ms!");
        });

        t.setDaemon(true);

        t.start();

        awaitNewCheckpoints(2, ignite);

        log.info("Time to stop ignite!");

        ((IgniteProcessProxy)grid(1)).kill();

        stopAllGrids();

        ignite = (IgniteEx)startGrids(2);

        ignite.cluster().active(true);

        checkSize(ignite.cache(CACHE_NAME));
    }

    private void populateData(IgniteCache<Integer, Person> cache) throws Exception {
        for (int i = 0; i < RECORDS_NUM; i++)
            cache.put(i, new Person(i, "bar-" + i));
    }

    private void checkSize(IgniteCache<Integer, Person> cache) throws Exception {
        assertEquals(RECORDS_NUM, cache.sizeLong());

        List<List<?>> results = cache.query(new SqlFieldsQuery("select id, name from Person")).getAll();

        assertEquals(RECORDS_NUM, results.size());

        for (int i = 0; i < RECORDS_NUM; i++)
            assertEquals("bar-" + i, cache.get(i).getName());

        for(List<?> row : results){
            assertEquals(2, row.size());

            assertEquals(cache.get((Integer) row.get(0)).getName(), row.get(1));
        }
    }

    private void awaitNewCheckpoints(int checkpointsNumber, IgniteEx ignite) throws Exception {
        for (int i = 0; i < checkpointsNumber; i++) {
            forceCheckpoint(ignite);

            ignite.compute(ignite.cluster().forRemotes()).call(new ForceCheckpointTask());

            Thread.sleep(100L);
        }
    }

    private static class ForceCheckpointTask implements IgniteCallable<Void> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        @Override public Void call() throws Exception {
            long time = U.currentTimeMillis();

            ((IgniteEx)ignite).context().cache().context().database().forceCheckpoint("test");

            System.out.println("Force checkpoint time: " + (U.currentTimeMillis() - time) + " ms");

            return null;
        }
    }

}