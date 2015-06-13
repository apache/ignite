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

package org.apache.ignite.spring.autowired;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;
import org.springframework.context.*;
import org.springframework.context.support.*;
import org.springframework.core.io.*;

import javax.cache.configuration.*;
import java.net.*;
import java.util.*;

/**
 * Spring autowiring test.
 */
public class SpringAutowiredSelfTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_NAME = SpringAutowiredSelfTest.class.getSimpleName();

    /** */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder();

    static {
        IP_FINDER.setAddresses(Arrays.asList("127.0.0.1:47500"));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        SpringAutowiredTestStore.load.clear();
        SpringAutowiredTestStore.write.clear();
        SpringAutowiredTestStore.delete.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStoreAtomic() throws Exception {
        testStore(CacheAtomicityMode.ATOMIC);

        assertEquals(3, SpringAutowiredTestStore.load.size());
        assertEquals(3, SpringAutowiredTestStore.write.size());
        assertEquals(3, SpringAutowiredTestStore.delete.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testStoreTransactional() throws Exception {
        testStore(CacheAtomicityMode.TRANSACTIONAL);

        assertEquals(3, SpringAutowiredTestStore.load.size());
        assertEquals(1, SpringAutowiredTestStore.write.size());
        assertEquals(1, SpringAutowiredTestStore.delete.size());
    }

    /**
     * @param mode Atomicity mode.
     * @throws Exception In case of error.
     */
    private void testStore(CacheAtomicityMode mode) throws Exception {
        ApplicationContext appCtx = new ClassPathXmlApplicationContext(
            "org/apache/ignite/spring/autowired/autowired.xml");

        for (int i = 0; i < 3; i++)
            IgniteSpring.start(configuration("server-" + i), appCtx);

        Ignition.setClientMode(true);

        Ignite client = IgniteSpring.start(configuration("client"), appCtx);

        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>();

        ccfg.setName(CACHE_NAME);
        ccfg.setAtomicityMode(mode);
        ccfg.setCacheStoreFactory(FactoryBuilder.factoryOf(SpringAutowiredTestStore.class));
        ccfg.setReadThrough(true);
        ccfg.setWriteThrough(true);

        IgniteCache<Integer, Integer> cache = client.createCache(ccfg);

        for (int i = 0; i < 100; i++)
            cache.get(i);

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        for (int i = 0; i < 100; i++)
            cache.remove(i);
    }

    /**
     * @throws Exception If failed.
     */
    public void testServiceAtomic() throws Exception {
        ApplicationContext appCtx = new ClassPathXmlApplicationContext(
            "org/apache/ignite/spring/autowired/autowired.xml");

        for (int i = 0; i < 3; i++)
            IgniteSpring.start(configuration("server-" + i), appCtx);

        SpringAutowiredTestService.mode = CacheAtomicityMode.ATOMIC;

        URL url = new ClassPathResource("org/apache/ignite/spring/autowired/autowired-service.xml").getURL();

        SpringAutowiredTestService svc = Ignition.loadSpringBean(url, "test-service");

        svc.run();

        assertEquals(3, SpringAutowiredTestStore.load.size());
        assertEquals(3, SpringAutowiredTestStore.write.size());
        assertEquals(3, SpringAutowiredTestStore.delete.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testServiceTransactional() throws Exception {
        ApplicationContext appCtx = new ClassPathXmlApplicationContext(
            "org/apache/ignite/spring/autowired/autowired.xml");

        for (int i = 0; i < 3; i++)
            IgniteSpring.start(configuration("server-" + i), appCtx);

        SpringAutowiredTestService.mode = CacheAtomicityMode.TRANSACTIONAL;

        URL url = new ClassPathResource("org/apache/ignite/spring/autowired/autowired-service.xml").getURL();

        SpringAutowiredTestService svc = Ignition.loadSpringBean(url, "test-service");

        svc.run();

        assertEquals(3, SpringAutowiredTestStore.load.size());
        assertEquals(1, SpringAutowiredTestStore.write.size());
        assertEquals(1, SpringAutowiredTestStore.delete.size());
    }

    /**
     * @param name Name.
     * @return Configuration.
     */
    private IgniteConfiguration configuration(String name) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName(name);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }
}
