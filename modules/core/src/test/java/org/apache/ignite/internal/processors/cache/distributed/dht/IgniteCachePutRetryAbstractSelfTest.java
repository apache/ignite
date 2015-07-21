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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.testframework.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 *
 */
public abstract class IgniteCachePutRetryAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /**
     * @return Keys count for the test.
     */
    protected abstract int keysCount();

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setBackups(1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        AtomicConfiguration acfg = new AtomicConfiguration();

        acfg.setBackups(1);

        cfg.setAtomicConfiguration(acfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPut() throws Exception {
        final AtomicBoolean finished = new AtomicBoolean();

        int keysCnt = keysCount();

        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                while (!finished.get()) {
                    stopGrid(3);

                    U.sleep(300);

                    startGrid(3);
                }

                return null;
            }
        });


        try {
            for (int i = 0; i < keysCnt; i++)
                ignite(0).cache(null).put(i, i);

            finished.set(true);

            fut.get();
        }
        finally {
            finished.set(true);
        }

        for (int i = 0; i < keysCnt; i++)
            assertEquals(i, ignite(0).cache(null).get(i));
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailWithNoRetries() throws Exception {
        final AtomicBoolean finished = new AtomicBoolean();

        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                while (!finished.get()) {
                    stopGrid(3);

                    U.sleep(300);

                    startGrid(3);
                }

                return null;
            }
        });

        try {
            int keysCnt = keysCount();

            boolean eThrown = false;

            for (int i = 0; i < keysCnt; i++) {
                try {
                    ignite(0).cache(null).withNoRetries().put(i, i);
                }
                catch (Exception e) {
                    assertTrue("Invalid exception: " + e, X.hasCause(e, ClusterTopologyCheckedException.class) ||
                        X.hasCause(e, CachePartialUpdateException.class));

                    eThrown = true;

                    break;
                }
            }

            assertTrue(eThrown);

            finished.set(true);

            fut.get();
        }
        finally {
            finished.set(true);
        }
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 3 * 60 * 1000;
    }
}
