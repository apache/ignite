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

package org.apache.ignite.internal.processors.cache.multijvm;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * TODO: Add class description.
 */
public class GridCachePartitionedMultiJvmFullApiSelfTest extends GridCachePartitionedMultiNodeFullApiSelfTest {
    /** Local ignite. */
    private Ignite locIgnite;

    /** */
    private CountDownLatch allNodesJoinLatch;

    /** */
    private final IgnitePredicate<Event> nodeJoinLsnr = new IgnitePredicate<Event>() {
        @Override public boolean apply(Event evt) {
            allNodesJoinLatch.countDown();

            return true;
        }
    };

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        allNodesJoinLatch = new CountDownLatch(gridCount() - 1);

        super.beforeTestsStarted();

        assert allNodesJoinLatch.await(5, TimeUnit.SECONDS);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        IgniteExProcessProxy.killAll();

        locIgnite = null;

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        IgniteExProcessProxy.killAll(); // TODO: remove processes killing from here.

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IgniteNodeRunner.ipFinder);

        Map<IgnitePredicate<? extends Event>, int[]> lsnrs = new HashMap<>();

        lsnrs.put(nodeJoinLsnr, new int[] {EventType.EVT_NODE_JOINED});

        cfg.setLocalEventListeners(lsnrs);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        return super.cacheConfiguration(gridName); // TODO: CODE: implement.
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    protected boolean isMultiJvm() {
        return true;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllRemoveAll() throws Exception {
        // TODO: uncomment.
//        for (int i = 0; i < gridCount(); i++) {
//            IgniteEx grid0 = grid0(i);
//
//            info(">>>>> Grid" + i + ": " + grid0.localNode().id());
//        }

        Map<Integer, Integer> putMap = new LinkedHashMap<>();

        int size = 100;

        for (int i = 0; i < size; i++)
            putMap.put(i, i * i);

        IgniteCache<Object, Object> c0 = grid(0).cache(null);
        IgniteCache<Object, Object> c1 = grid(1).cache(null);

        c0.putAll(putMap);

        atomicClockModeDelay(c0);

        c1.removeAll(putMap.keySet());

        for (int i = 0; i < size; i++) {
            assertNull(c0.get(i));
            assertNull(c1.get(i));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutRemove() throws Exception {
        IgniteCache<Object, Object> c0 = grid(0).cache(null);
        IgniteCache<Object, Object> c1 = grid(1).cache(null);

        final int key = 1;
        final int val = 3;

        c0.put(key, val);

        assertEquals(val, c0.get(key));
        assertEquals(val, c1.get(key));

        assertTrue(c1.remove(key));

        U.sleep(1_000);

        assertTrue(c0.get(key) == null || c1.get(key) == null);
        assertNull(c1.get(key));
        assertNull(c0.get(key));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutRemove2() throws Exception {
        IgniteCache<Object, Object> c0 = grid(0).cache(null);
        IgniteCache<Object, Object> c1 = grid(1).cache(null);

        final int key = 1;
        final int val = 3;

        c1.put(key, val);

        assertEquals(val, c1.get(key));
        assertEquals(val, c0.get(key));

        assertTrue(c1.remove(key));

        U.sleep(1_000);

        assertNull(c1.get(key));
        assertNull(c0.get(key));
    }
}
