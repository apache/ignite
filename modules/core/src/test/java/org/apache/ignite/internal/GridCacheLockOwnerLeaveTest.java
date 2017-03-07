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

package org.apache.ignite.internal;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.internal.processors.datastructures.GridCacheInternalKeyImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Create lock after owner node left topology test and work with it from another nodes
 */
@GridCommonTest(group = "Kernal Self")
public class GridCacheLockOwnerLeaveTest extends GridCommonAbstractTest {
    /** */
    private static final int NODE_CNT = 4;

    /** */
    private static final int TEST_CNT = 5;

    public void test() throws Exception {
        for (int i = 0; i < TEST_CNT; i++) {
            lockOwnerLeavesGrid();
        }
    }
    /**
     * @throws Exception if failed.
     */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    public void lockOwnerLeavesGrid() throws Exception {
        startGrids(NODE_CNT);

        try {
            // Ensure there are no extra copies of the DS.
            awaitPartitionMapExchange();

            final String name = generateName(ignite(0));

            IgniteLock lock = ignite(0).reentrantLock(name, true, true, true);

            info("Acquiring lock on ignite: " + 0);

            lock.lock();

            info("Acquired lock on ignite: " + 0);

            Collection<IgniteInternalFuture<Object>> futs = new ArrayList<>(NODE_CNT - 1);

            for (int i = 1; i < 4; i++) {
                final int gridNo = i;

                final AtomicBoolean single = new AtomicBoolean();

                IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        IgniteLock lock = ignite(gridNo).reentrantLock(name, true, true, true);

                        info("Acquiring lock on ignite: " + gridNo);
                        lock.lock();

                        try {
                            info("Acquired lock on ignite: " + gridNo);

                            assertTrue(single.compareAndSet(false, true));

                            assertTrue(single.compareAndSet(true, false));

                            U.sleep(500);
                        }
                        finally {
                            lock.unlock();
                        }

                        return null;
                    }
                });

                futs.add(fut);
            }

            info("Stopping grid:" + 0);
            stopGrid(0);

            info("Stopped grid:" + 0);
            for (IgniteInternalFuture<Object> fut : futs)
                fut.get();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param ig Ignite to map the key for.
     * @return Generated lock name that is guaranteed to be mapped on the given Ignite instance.
     */
    private String generateName(Ignite ig) {
        IgniteEx igK = (IgniteEx)ig;

        Affinity<Object> aff = igK.context().cache().cache("ignite-atomics-sys-cache").affinity();

        while (true) {
            String name = UUID.randomUUID().toString();

            if (aff.mapKeyToNode(new GridCacheInternalKeyImpl(name)).isLocal())
                return name;
        }
    }
}