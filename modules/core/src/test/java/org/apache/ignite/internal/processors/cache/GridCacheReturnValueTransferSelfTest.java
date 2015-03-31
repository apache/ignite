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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.testframework.junits.common.*;

import javax.cache.processor.*;
import java.io.*;
import java.util.*;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.*;
import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.internal.processors.cache.CacheFlag.*;

/**
 * Tests transform for extra traffic.
 */
public class GridCacheReturnValueTransferSelfTest extends GridCommonAbstractTest {
    /** Distribution mode. */
    private boolean cache;

    /** Atomicity mode. */
    private CacheAtomicityMode atomicityMode;

    /** Atomic write order mode. */
    private CacheAtomicWriteOrderMode writeOrderMode;

    /** Number of backups. */
    private int backups;

    /** Fail deserialization flag. */
    private static volatile boolean failDeserialization;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setBackups(backups);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setAtomicWriteOrderMode(writeOrderMode);

        cfg.setCacheConfiguration(ccfg);

        if (!cache)
            cfg.setClientMode(true);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformAtomicPrimaryNoBackups() throws Exception {
        checkTransform(ATOMIC, PRIMARY, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformAtomicClockNoBackups() throws Exception {
        checkTransform(ATOMIC, CLOCK, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformAtomicPrimaryOneBackup() throws Exception {
        checkTransform(ATOMIC, PRIMARY, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformAtomicClockOneBackup() throws Exception {
        checkTransform(ATOMIC, CLOCK, 1);
    }

    /**
     * @throws Exception If failed.
     * TODO gg-8273 enable when fixed
     */
    public void _testTransformTransactionalNoBackups() throws Exception {
        checkTransform(TRANSACTIONAL, PRIMARY, 0);
    }

    /**
     * @throws Exception If failed.
     * TODO gg-8273 enable when fixed
     */
    public void _testTransformTransactionalOneBackup() throws Exception {
        checkTransform(TRANSACTIONAL, PRIMARY, 1);
    }

    /**
     * @param mode Atomicity mode.
     * @param order Atomic cache write order mode.
     * @param b Number of backups.
     * @throws Exception If failed.
     */
    private void checkTransform(CacheAtomicityMode mode, CacheAtomicWriteOrderMode order, int b)
        throws Exception {
        try {
            atomicityMode = mode;

            backups = b;

            writeOrderMode = order;

            cache = true;

            startGrids(2);

            cache = false;

            startGrid(2);

            failDeserialization = false;

            // Get client grid.
            IgniteCache<Integer, TestObject> cache = grid(2).cache(null);

            if (backups > 0 && atomicityMode == ATOMIC)
                cache = ((IgniteCacheProxy<Integer, TestObject>)cache).flagOn(FORCE_TRANSFORM_BACKUP);

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
            entry.setValue(new TestObject());

            return null;
        }
    }

    /**
     *
     */
    @IgniteImmutable
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
            assert !failDeserialization;
        }
    }
}
