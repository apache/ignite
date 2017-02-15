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

package org.apache.ignite.transactions.spring;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.InvalidIsolationLevelException;

/**
 * Spring transaction test.
 */
public class GridSpringTransactionManagerSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME = "testCache";

    /** */
    private GridSpringTransactionService service;

    /**
     * {@inheritDoc}
     */
    @Override
    protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cache = new CacheConfiguration();

        cache.setName(CACHE_NAME);
        cache.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        cfg.setCacheConfiguration(cache);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getTestGridName() {
        return "testGrid";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void beforeTestsStarted() throws Exception {
        startGrid();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void beforeTest() throws Exception {
        ApplicationContext applicationContext = new GenericXmlApplicationContext("config/spring-transactions.xml");

        service = (GridSpringTransactionService)applicationContext.getBean("gridSpringTransactionService");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void afterTest() throws Exception {
        grid().cache(CACHE_NAME).removeAll();
    }

    /** */
    public void testSuccessPut() {
        IgniteCache<Integer, String> c = grid().cache(CACHE_NAME);

        int entryCnt = 1_000;

        service.put(c, entryCnt);

        assertEquals(entryCnt, c.size());
    }

    /** */
    public void testFailPut() {
        IgniteCache<Integer, String> c = grid().cache(CACHE_NAME);

        int entryCnt = 1_000;

        try {
            service.putWithError(c, entryCnt);
        }
        catch (Exception ignored) {
            // No-op.
        }

        assertEquals(0, c.size());
    }

    /** */
    public void testMandatoryPropagation() {
        IgniteCache<Integer, String> c = grid().cache(CACHE_NAME);

        try {
            service.putWithMandatoryPropagation(c);
        }
        catch (IllegalTransactionStateException e) {
            assertEquals("No existing transaction found for transaction marked with propagation 'mandatory'", e.getMessage());
        }

        assertEquals(0, c.size());
    }

    /** */
    public void testUnsupportedIsolationLevel() {
        IgniteCache<Integer, String> c = grid().cache(CACHE_NAME);

        try {
            service.putWithUnsupportedIsolationLevel(c);
        }
        catch (InvalidIsolationLevelException e) {
            assertEquals("Ignite does not support READ_UNCOMMITTED isolation level.", e.getMessage());
        }

        assertEquals(0, c.size());
    }
}
