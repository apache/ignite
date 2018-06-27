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

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_EXCHANGE_HISTORY_SIZE;

/**
 * Test exchange history size parameter effect.
 */
public class GridCachePartitionExchangeManagerHistSizeTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private String oldHistVal;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        oldHistVal = System.getProperty(IGNITE_EXCHANGE_HISTORY_SIZE);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        if (oldHistVal != null)
            System.setProperty(IGNITE_EXCHANGE_HISTORY_SIZE, oldHistVal);
        else
            System.clearProperty(IGNITE_EXCHANGE_HISTORY_SIZE);
    }


    /**
     * @throws Exception If failed.
     */
    public void testSingleExchangeHistSize() throws Exception {
        System.setProperty(IGNITE_EXCHANGE_HISTORY_SIZE, "1");

        startGridsMultiThreaded(10);
    }
}
