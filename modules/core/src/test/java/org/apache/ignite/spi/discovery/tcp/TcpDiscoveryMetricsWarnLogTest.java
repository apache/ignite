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

package org.apache.ignite.spi.discovery.tcp;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISCOVERY_METRICS_QNT_WARN;

/**
 * Class for testing warning log message about too many cache metrics.
 */
public class TcpDiscoveryMetricsWarnLogTest extends GridCommonAbstractTest {
    /** Listener log messages. */
    private static ListeningTestLogger testLog;

    /** Desired message. */
    public static final String LOG_MSG = "To prevent Discovery blocking use";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        testLog = new ListeningTestLogger(false, log);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        testLog.clearListeners();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setMetricsUpdateFrequency(500L)
            .setGridLogger(testLog);
    }

    /**
     * Test checks that the desired message occurs in logs.
     *
     * @throws Exception If any error occurs.
     */
    @Test
    @WithSystemProperty(key = IGNITE_DISCOVERY_METRICS_QNT_WARN, value = "20")
    public void testMetricsWarningLog() throws Exception {
        testLog.warning("IGNITE_DISCOVERY_METRICS_QNT_WARN = "
            + System.getProperty(IGNITE_DISCOVERY_METRICS_QNT_WARN));

        LogListener logLsnr0 = LogListener.matches(LOG_MSG)
            .andMatches("TcpDiscoveryMetricsWarnLogTest0")
            .atLeast(1)
            .build();

        LogListener logLsnr1 = LogListener.matches(LOG_MSG)
            .andMatches("TcpDiscoveryMetricsWarnLogTest1")
            .atLeast(1)
            .build();

        LogListener logLsnr2 = LogListener.matches(LOG_MSG)
            .andMatches("TcpDiscoveryMetricsWarnLogTest2")
            .atLeast(1)
            .build();

        testLog.registerListener(logLsnr0);
        testLog.registerListener(logLsnr1);
        testLog.registerListener(logLsnr2);

        Ignite ignite0 = startGrid(0);

        startGrid(1);

        startClientGrid(2);

        for (int i = 1; i <= 30; i++)
            createAndFillCache(i, ignite0);

        awaitMetricsUpdate(3);

        assertTrue(logLsnr0.check());
        assertTrue(logLsnr1.check());
        assertTrue(logLsnr2.check());
    }

    @Test
    @WithSystemProperty(key = IGNITE_DISCOVERY_METRICS_QNT_WARN, value = "0")
    public void testMetricsWarningLog0() throws Exception {
        testMetricsWarningLog();
    }

    /**
     * Creates and fills cahes with test data.
     *
     * @param cacheNum Cache number to generate a cache name.
     * @param ignite Ignite instance to create a cache in.
     */
    private void createAndFillCache(int cacheNum, Ignite ignite) {
        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME + cacheNum).setStatisticsEnabled(true)
        );

        for (int i = 1; i < 100; i++)
            cache.put(i, i);
    }
}
