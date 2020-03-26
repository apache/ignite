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

package org.apache.ignite.internal.processors.cache;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Tests transform for extra traffic.
 */
public class GridCacheReturnValueTransferSelfTest extends GridCommonAbstractTest {
    /** Atomicity mode. */
    private CacheAtomicityMode atomicityMode;

    /** Number of backups. */
    private int backups;

    /** Fail deserialization flag. */
    private static volatile boolean failDeserialization;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setBackups(backups);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(atomicityMode);

        cfg.setCacheConfiguration(ccfg);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     * TODO IGNITE-581 enable when fixed.
     */
    @Test
    public void testTransformTransactionalNoBackups() throws Exception {
        // Test works too long and fails.
        checkTransform(TRANSACTIONAL, 0);
    }

    /**
     * @throws Exception If failed.
     * TODO IGNITE-581 enable when fixed.
     */
    @Test
    public void testTransformTransactionalOneBackup() throws Exception {
        // Test works too long and fails.
        checkTransform(TRANSACTIONAL, 1);
    }

    /**
     * @param mode Atomicity mode.
     * @param b Number of backups.
     * @throws Exception If failed.
     */
    private void checkTransform(CacheAtomicityMode mode, int b)
        throws Exception {
        try {
            atomicityMode = mode;

            backups = b;

            startGrids(2);
            startClientGrid(2);

            failDeserialization = false;

            // Get client grid.
            IgniteCache<Integer, TestObject> cache = grid(2).cache(DEFAULT_CACHE_NAME);

            for (int i = 0; i < 100; i++)
                cache.put(i, new TestObject());

            failDeserialization = true;

            info(">>>>>> Transforming");

            // Transform (check non-existent keys also).
            for (int i = 0; i < 200; i++)
                cache.invoke(i, new Transform());

            Set<Integer> keys = new HashSet<>();

            // Check transformAll.
            for (int i = 0; i < 300; i++)
                keys.add(i);

            cache.invokeAll(keys, new Transform());

            // Avoid errors during stop.
            failDeserialization = false;
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private static class Transform implements EntryProcessor<Integer, TestObject, Void>, Serializable {
        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Integer, TestObject> entry, Object... args) {
            if (entry.getKey() < 100)
                assertNotNull(entry.getValue());
            else
                assertNull(entry.getValue());

            return null;
        }
    }

    /**
     *
     */
    private static class TestObject implements Externalizable {
        /**
         *
         */
        public TestObject() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            assert !failDeserialization;
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            // No-op.
        }
    }
}
