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

package org.apache.ignite.stream;

import org.apache.ignite.*;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;


public class GridStreamTransformerTest extends GridCommonAbstractTest {

    private static int PORT = 47500;
    private static String LOCALHOST = "localhost";

    private Ignite grid = null;

    /**
     * {@inheritDoc}
     */
    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration configuration = new IgniteConfiguration();
        configuration.setClientMode(false);
        configuration.setIgniteInstanceName(igniteInstanceName);
        TcpDiscoveryVmIpFinder finder = new TcpDiscoveryVmIpFinder();
        finder.registerAddresses(Collections.singletonList(new InetSocketAddress(LOCALHOST, PORT)));
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        spi.setLocalPort(PORT);
        spi.setLocalPortRange(0);
        spi.setIpFinder(finder);
        configuration.setDiscoverySpi(spi);
        return configuration;
    }

    public static class StreamingExampleCacheEntryProcessor implements CacheEntryProcessor<String, Long, Object> {
        @Override
        public Object process(MutableEntry<String, Long> e, Object... arg) throws EntryProcessorException {
            System.out.println("Executed");
            Long val = e.getValue();
            e.setValue(val == null ? 1L : val + 1);
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void beforeTestsStarted() throws Exception {
        grid = startGrid();
    }

    public void testStreamTransformer() throws IgniteException, IOException, IgniteCheckedException {
        Ignition.setClientMode(true);

        IgniteCache<String, Long> stmCache = grid.getOrCreateCache("mycache");
        try (IgniteDataStreamer<String, Long> stmr = grid.dataStreamer(stmCache.getName())) {
            stmr.allowOverwrite(true);
            stmr.receiver(StreamTransformer.from(new StreamingExampleCacheEntryProcessor()));
            stmr.addData("word", 1L);
            System.out.println("Finished");
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }
}