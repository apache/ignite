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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK;
import static org.apache.ignite.binary.BinaryStringEncoding.ENC_NAME_WINDOWS_1251;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Put-get tests for nodes with distinct binary string encodings.
 */
@SuppressWarnings("unchecked")
public class GridCacheMixedBinaryStringEncodingsTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String WIN1251_NODE = "windows-1251-node";

    /** */
    private static final String DEFAULT_UTF8_NODE = "default-utf-8-node";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setClientMode(false);

        CacheConfiguration cCfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        cCfg.setCacheMode(REPLICATED);
        cCfg.setWriteSynchronizationMode(FULL_SYNC);
        cCfg.setRebalanceMode(SYNC);

        cfg.setCacheConfiguration(cCfg);

        BinaryConfiguration bCfg = cfg.getBinaryConfiguration();

        if (bCfg == null) {
            bCfg = new BinaryConfiguration();

            cfg.setBinaryConfiguration(bCfg);
        }

        if (igniteInstanceName.equals(WIN1251_NODE))
            bCfg.setEncoding(ENC_NAME_WINDOWS_1251);

        cfg.setMarshaller(new BinaryMarshaller());

        return cfg;
    }

    /** */
    protected void beforeTest() throws Exception {
        System.setProperty(IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK, "true");
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetDefaultString() throws Exception {
        doTestPutGet(DEFAULT_UTF8_NODE, WIN1251_NODE, true, "Новгород", "Chicago", "Düsseldorf", "北京市");
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetEncoded() throws Exception {
        doTestPutGet(WIN1251_NODE, DEFAULT_UTF8_NODE, true, "Новгород", "Chicago");
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetEncodedUnsupportedChars() throws Exception {
        doTestPutGet(WIN1251_NODE, DEFAULT_UTF8_NODE, false, "Düsseldorf", "北京市");
    }

    /** */
    private void doTestPutGet(String nodeName0, String nodeName1, boolean shouldMatch, String... data)
        throws Exception {
        try {
            Ignite node = startGrid(nodeName0);

            startGrid(nodeName1);

            IgniteCache cache = node.cache(DEFAULT_CACHE_NAME);

            for (int i = 0; i < data.length; i++)
                cache.put(i, data[i]);

            stopGrid(nodeName0);

            IgniteCache cacheAgain = startGrid(nodeName0).getOrCreateCache(DEFAULT_CACHE_NAME);

            for (int i = 0; i < data.length; i++)
                assertEquals(shouldMatch, data[i].equals(cacheAgain.get(i)));
        }
        finally {
            stopAllGrids();
        }
    }
}
