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

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class GridCacheRabalancingPartitionMapExchangeSelfTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private final ConcurrentHashMap<String, AtomicInteger> map = new ConcurrentHashMap<>();

    /** */
    private volatile boolean record = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration iCfg = super.getConfiguration(gridName);

        TcpCommunicationSpi commSpi = new CountingCommunicationSpi();

        commSpi.setLocalPort(GridTestUtils.getNextCommPort(getClass()));
        commSpi.setTcpNoDelay(true);

        iCfg.setCommunicationSpi(commSpi);

        return iCfg;
    }

    /**
     *
     */
    public class CountingCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(final ClusterNode node, final Message msg,
            final IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            final Object msg0 = ((GridIoMessage)msg).message();

            recordMessage(msg0);

            super.sendMessage(node, msg, ackC);
        }
    }

    /**
     * @param msg
     */
    private void recordMessage(Object msg) {
        if (record) {
            String id = msg.getClass().toString();

            if (msg instanceof GridDhtPartitionsFullMessage)
                id += ((GridDhtPartitionsFullMessage)msg).exchangeId();

            int size = 0;

            try {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutput out = new ObjectOutputStream(bos);
                out.writeObject(msg);

                size = bos.toByteArray().length;
            }
            catch (IOException e) {
            }

            AtomicInteger ai = map.get("cnt " + id);

            if (ai == null) {
                ai = new AtomicInteger();

                AtomicInteger oldAi = map.putIfAbsent("cnt " + id, ai);

                (oldAi != null ? oldAi : ai).addAndGet(size);
            }
            else
                ai.addAndGet(size);

            ai = map.get("size" + id);

            if (ai == null) {
                ai = new AtomicInteger();

                AtomicInteger oldAi = map.putIfAbsent("size" + id, ai);

                (oldAi != null ? oldAi : ai).incrementAndGet();
            }
            else
                ai.incrementAndGet();
        }
    }

    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE;
    }

    /**
     * @throws Exception e.
     */
    public void test() throws Exception {
        record = false;

        startGrids(10);

        awaitPartitionMapExchange(true);

        for (int i = 0; i < 10; i++) {
            CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>();

            cfg.setName("cache" + i);
            cfg.setCacheMode(CacheMode.PARTITIONED);
            cfg.setRebalanceMode(CacheRebalanceMode.SYNC);
            cfg.setBackups(1);

            ignite(0).getOrCreateCache(cfg);
        }

        awaitPartitionMapExchange(true);

        U.sleep(60_000);

        System.out.println("--------------------------TESTING--------------------------");

        record = true;

        U.sleep(60_000);

        record = false;

        for (Map.Entry entry : map.entrySet()) {
            System.out.println(entry.getKey().toString() + " ------ " + entry.getValue().toString());
        }

        IgniteKernal ignite = ((IgniteKernal)grid(0).cluster().forOldest().ignite());

        ignite.dumpDebugInfo();

    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

}
