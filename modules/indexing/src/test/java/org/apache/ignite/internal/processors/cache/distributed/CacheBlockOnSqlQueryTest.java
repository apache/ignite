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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Random;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class CacheBlockOnSqlQueryTest extends CacheBlockOnReadAbstractTest {
    /** {@inheritDoc} */
    @Override @NotNull protected CacheReadBackgroundOperation<?, ?> getReadOperation() {
        return new CacheReadBackgroundOperation<Integer, TestingEntity>() {
            /** Random. */
            private Random random = new Random();

            /** {@inheritDoc} */
            @Override protected CacheConfiguration<Integer, TestingEntity> createCacheConfiguration() {
                return super.createCacheConfiguration().setIndexedTypes(Integer.class, TestingEntity.class);
            }

            /** {@inheritDoc} */
            @Override protected Integer createKey(int idx) {
                return idx;
            }

            /** {@inheritDoc} */
            @Override protected TestingEntity createValue(int idx) {
                return new TestingEntity(idx, idx);
            }

            /** {@inheritDoc} */
            @Override public void doRead() {
                int idx = random.nextInt(entriesCount());

                cache().query(
                    new SqlQuery<>(TestingEntity.class, "val >= ? and val < ?")
                        .setArgs(idx, idx + 500)
                ).getAll();
            }
        };
    }

    /**
     *
     */
    public static class TestingEntity {
        /** Id. */
        @QuerySqlField(index = true)
        public Integer id;

        /** Value. */
        @QuerySqlField(index = true)
        public double val;

        /**
         * Default constructor.
         */
        public TestingEntity() {
        }

        /**
         * @param id Id.
         * @param val Value.
         */
        public TestingEntity(Integer id, double val) {
            this.id = id;
            this.val = val;
        }
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9916")
    @Test
    @Override public void testStartServerAtomicPartitioned() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9916")
    @Test
    @Override public void testStartServerTransactionalPartitioned() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9916")
    @Test
    @Override public void testStopServerAtomicPartitioned() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9916")
    @Test
    @Override public void testStopServerTransactionalPartitioned() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9916")
    @Test
    @Override public void testStopBaselineAtomicPartitioned() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9916")
    @Test
    @Override public void testStopBaselineTransactionalPartitioned() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9916")
    @Test
    @Override public void testStartClientAtomicPartitioned() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9916")
    @Test
    @Override public void testStartClientTransactionalPartitioned() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = ATOMIC, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9916")
    @Test
    @Override public void testStopClientAtomicPartitioned() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Params(baseline = 1, atomicityMode = TRANSACTIONAL, cacheMode = PARTITIONED)
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-9916")
    @Test
    @Override public void testStopClientTransactionalPartitioned() {
        // No-op.
    }
}
