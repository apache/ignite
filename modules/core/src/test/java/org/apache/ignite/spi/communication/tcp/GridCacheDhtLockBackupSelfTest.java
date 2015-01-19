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

package org.apache.ignite.spi.communication.tcp;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.jdk.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.apache.ignite.spi.communication.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Special cases for GG-2329.
 */
public class GridCacheDhtLockBackupSelfTest extends GridCommonAbstractTest {
    /** Ip-finder. */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Communication spi for grid start. */
    private CommunicationSpi commSpi;

    /** Marshaller used in test. */
    private IgniteMarshaller marsh = new IgniteJdkMarshaller();

    /**
     *
     */
    public GridCacheDhtLockBackupSelfTest() {
        super(false /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(cacheConfiguration());

        cfg.setMarshaller(marsh);

        assert commSpi != null;

        cfg.setCommunicationSpi(commSpi);

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(GridCacheMode.PARTITIONED);
        cacheCfg.setWriteSynchronizationMode(FULL_ASYNC);
        cacheCfg.setPreloadMode(SYNC);

        return cacheCfg;
    }

    /**
     * @throws Exception If test failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public void testLock() throws Exception {
        final int kv = 1;

        Ignite ignite1 = startGridWithSpi(1, new TestCommunicationSpi(GridNearUnlockRequest.class, 1000));

        Ignite ignite2 = startGridWithSpi(2, new TestCommunicationSpi(GridNearUnlockRequest.class, 1000));

        if (!ignite1.cluster().mapKeyToNode(null, kv).id().equals(ignite1.cluster().localNode().id())) {
            Ignite tmp = ignite1;
            ignite1 = ignite2;
            ignite2 = tmp;
        }

        // Now, grid1 is always primary node for key 1.
        final IgniteCache<Integer, String> cache1 = ignite1.jcache(null);
        final IgniteCache<Integer, String> cache2 = ignite2.jcache(null);

        info(">>> Primary: " + ignite1.cluster().localNode().id());
        info(">>>  Backup: " + ignite2.cluster().localNode().id());

        final CountDownLatch l1 = new CountDownLatch(1);

        Thread t1 = new GridTestThread(new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                info("Before lock for key: " + kv);

                cache1.lock(kv).lock();

                info("After lock for key: " + kv);

                try {
                    assert cache1.isLocked(kv);
                    assert cache1.isLockedByThread(kv);

                    l1.countDown();

                    info("Let thread2 proceed.");

                    cache1.put(kv, Integer.toString(kv));

                    info("Put " + kv + '=' + Integer.toString(kv) + " key pair into cache.");
                }
                finally {
                    Thread.sleep(1000);

                    cache1.lockAll(Collections.singleton(kv)).unlock();

                    info("Unlocked key in thread 1: " + kv);
                }

                assert !cache1.isLockedByThread(kv);

                return null;
            }
        });

        Thread t2 = new GridTestThread(new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                info("Waiting for latch1...");

                l1.await();

                cache2.lock(kv).lock();

                try {
                    String v = cache2.get(kv);

                    assert v != null : "Value is null for key: " + kv;
                    assertEquals(Integer.toString(kv), v);
                }
                finally {
                    cache2.lockAll(Collections.singleton(kv)).unlock();

                    info("Unlocked key in thread 2: " + kv);
                }

                assert !cache2.isLockedByThread(kv);

                return null;
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        info("Before remove all");

        cache1.flagsOn(GridCacheFlag.SYNC_COMMIT).removeAll();

        info("Remove all completed");

        if (cache2.size() > 0) {
            String failMsg = cache2.toString();

            long start = System.currentTimeMillis();

            while (cache2.size() > 0)
                U.sleep(100);

            long clearDuration = System.currentTimeMillis() - start;

            assertTrue("Cache on backup is not empty (was cleared in " + clearDuration + "ms): " + failMsg,
                clearDuration < 3000);
        }
    }

    /**
     * Starts grid with given communication spi set in configuration.
     *
     * @param idx Grid index.
     * @param commSpi Communication spi.
     * @return Started grid.
     * @throws Exception If grid start failed.
     */
    private Ignite startGridWithSpi(int idx, CommunicationSpi commSpi) throws Exception {
        this.commSpi = commSpi;

        try {
            return startGrid(idx);
        }
        finally {
            this.commSpi = null;
        }
    }

    /**
     * Test communication spi that delays message sending.
     */
    private class TestCommunicationSpi extends TcpCommunicationSpi {
        /** Class of delayed messages. */
        private Class<?> delayedMsgCls;

        /** */
        private int delayTime;

        /**
         * Creates test communication spi.
         *
         * @param delayedMsgCls Messages of this class will be delayed.
         * @param delayTime Time to be delayed.
         */
        private TestCommunicationSpi(Class delayedMsgCls, int delayTime) {
            this.delayedMsgCls = delayedMsgCls;
            this.delayTime = delayTime;
        }

        /**
         * Checks message and awaits when message is allowed to be sent if it is a checked message.
         *
         * @param obj Message being  sent.
         * @param srcNodeId Sender node id.
         */
        private void checkAwaitMessageType(GridTcpCommunicationMessageAdapter obj, UUID srcNodeId) {
            try {
                GridIoMessage plainMsg = (GridIoMessage)obj;

                Object msg = plainMsg.message();

                if (delayedMsgCls.isAssignableFrom(msg.getClass())) {
                    info(getSpiContext().localNode().id() + " received message from " + srcNodeId);

                    U.sleep(delayTime);
                }
            }
            catch (IgniteCheckedException e) {
                U.error(log, "Cannot process incoming message", e);
            }
        }

        /** {@inheritDoc} */
        @Override protected void notifyListener(UUID sndId, GridTcpCommunicationMessageAdapter msg,
            IgniteRunnable msgC) {
            checkAwaitMessageType(msg, sndId);

            super.notifyListener(sndId, msg, msgC);
        }
    }
}
