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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicWriteOrderMode.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheFlag.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Tests transform for extra traffic.
 */
public class GridCacheReturnValueTransferSelfTest extends GridCommonAbstractTest {
    /** Distribution mode. */
    private GridCacheDistributionMode distroMode;

    /** Atomicity mode. */
    private GridCacheAtomicityMode atomicityMode;

    /** Atomic write order mode. */
    private GridCacheAtomicWriteOrderMode writeOrderMode;

    /** Number of backups. */
    private int backups;

    /** Fail deserialization flag. */
    private static volatile boolean failDeserialization;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration ccfg = new GridCacheConfiguration();

        ccfg.setBackups(backups);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setAtomicWriteOrderMode(writeOrderMode);

        ccfg.setDistributionMode(distroMode);

        cfg.setCacheConfiguration(ccfg);

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
     * @throws Exception If failed.
     */
    private void checkTransform(GridCacheAtomicityMode mode, GridCacheAtomicWriteOrderMode order, int b)
        throws Exception {
        try {
            atomicityMode = mode;

            backups = b;

            writeOrderMode = order;

            distroMode = GridCacheDistributionMode.PARTITIONED_ONLY;

            startGrids(2);

            distroMode = GridCacheDistributionMode.CLIENT_ONLY;

            startGrid(2);

            failDeserialization = false;

            // Get client grid.
            GridCacheProjection<Integer, TestObject> cache = grid(2).cache(null);

            if (backups > 0 && atomicityMode == ATOMIC)
                cache = cache.flagsOn(FORCE_TRANSFORM_BACKUP);

            for (int i = 0; i < 100; i++)
                cache.put(i, new TestObject());

            failDeserialization = true;

            info(">>>>>> Transforming");

            // Transform (check non-existent keys also.
            for (int i = 0; i < 200; i++)
                cache.transform(i, new Transform());

            Map<Integer, Transform> transformMap = new HashMap<>();

            // Check transformAll.
            for (int i = 0; i < 300; i++)
                transformMap.put(i, new Transform());

            cache.transformAll(transformMap);

            // Avoid errors during stop.
            failDeserialization = false;
        }
        finally {
            stopAllGrids();
        }
    }

    private static class Transform implements IgniteClosure<TestObject, TestObject> {
        /** {@inheritDoc} */
        @Override public TestObject apply(TestObject testObject) {
            return new TestObject();
        }
    }

    /**
     *
     */
    private static class TestObject implements Externalizable {
        public TestObject() {
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
