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

package org.apache.ignite.internal.processors.performancestatistics;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Test system views for SQL.  */
public class PerformanceStatisticsSystemViewTablesTest extends AbstractPerformanceStatisticsTest {
    /** */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger();

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setCacheConfiguration(defaultCacheConfiguration());
        cfg.setGridLogger(listeningLog);
        return cfg;
    }

    /** @throws Exception If failed. */
    @Test
    public void testSystemViewTables() throws Exception {
        LogListener lsnr = LogListener.matches("Performance statistics writer started.").build();
        listeningLog.registerListener(lsnr);

        try (IgniteEx ignite = startGrid(0)) {
            CacheConfiguration<Long, Person> personCacheCfg = new CacheConfiguration<>();
            personCacheCfg.setName("Person");

            QueryEntity qryEntity = new QueryEntity(Long.class, Person.class)
                .addQueryField("id", Long.class.getName(), null).addQueryField("age", Integer.class.getName(), null)
                .addQueryField("salary", Float.class.getName(), null)
                .addQueryField("name", String.class.getName(), null);

            qryEntity.setIndexes(Arrays.asList(new QueryIndex("id"), new QueryIndex("salary", false)));

            personCacheCfg.setQueryEntities(List.of(qryEntity));

            IgniteCache<Long, Person> cache = ignite.createCache(personCacheCfg);
            cache.put(0L, new Person(1, "Alex", 3));

            startCollectStatistics();

            Set<Object> expectedIndexes = Set.of("PERSON_ID_ASC_IDX", "PERSON_SALARY_DESC_IDX");
            Set<Object> actualIndexes = new HashSet<>();

            Set<Object> expectedColumns = Set.of("SALARY", "AGE", "NAME");
            Set<Object> actualColumns = new HashSet<>();

            AtomicInteger tablesCnt = new AtomicInteger(0);

            assertTrue("Performance statistics writer did not start.", waitForCondition(lsnr::check, TIMEOUT));

            stopCollectStatisticsAndRead(new TestHandler() {
                @Override public void systemView(UUID id, String name, List<String> schema, List<Object> row) {
                    if ("table.columns".equals(name))
                        actualColumns.add(getViewByName(schema, row, "columnName"));

                    if ("indexes".equals(name))
                        actualIndexes.add(getViewByName(schema, row, "indexName"));

                    if ("tables".equals(name))
                        tablesCnt.incrementAndGet();
                }
            });

            assertTrue(actualColumns.containsAll(expectedColumns));
            assertTrue(actualIndexes.containsAll(expectedIndexes));
            assertEquals(1, tablesCnt.get());
        }
    }

    /** */
    private static class Person implements Serializable {
        /** */
        private final long id;

        /** */
        private final String name;

        /** */
        private final float salary;

        /** */
        private Person(long id, String name, float salary) {
            this.id = id;
            this.name = name;
            this.salary = salary;
        }

        /** */
        public long id() {
            return id;
        }

        /** */
        public String name() {
            return name;
        }

        /** */
        public float salary() {
            return salary;
        }
    }

    /** */
    private Object getViewByName(List<String> schema, List<Object> row, String attr) {
        int index = schema.indexOf(attr);
        return row.get(index);
    }
}
