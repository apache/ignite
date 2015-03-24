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

package org.apache.ignite.marshaller.optimized;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheMode.*;

/**
 *
 */
public class OptimizedMarshallerNodeFailoverTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean cache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setMarshaller(new OptimizedMarshaller());

        if (cache) {
            CacheConfiguration ccfg = new CacheConfiguration();

            ccfg.setCacheMode(PARTITIONED);

            ccfg.setBackups(1);

            cfg.setCacheConfiguration(ccfg);
        }
        else
            cfg.setClientMode(true);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassCacheUpdateFailover() throws Exception {
        cache = true;

        startGridsMultiThreaded(2);

        cache = false;

        IgniteCache<Integer, Object> cache0 = ignite(0).cache(null);

        for (int i = 0; i < 10; i++) {
            log.info("Iteration: " + i);

            Map<Integer, Object> map = new HashMap<>();

            for (int j = 0; j < 10_000; j++)
                map.put(j, create(i + 1));

            final Ignite ignite = startGrid(2);

            IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable() {
                @Override public Object call() throws Exception {
                    ignite.close();

                    return null;
                }
            });

            cache0.putAll(map); // Do not stop cache node, so put should not fail.

            fut.get();
        }

        cache = true;

        Ignite ignite = startGrid(2); // Check can start on more cache node.

        assertNotNull(ignite.cache(null));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @param id Class ID.
     * @return Test class instance.
     */
    private static Object create(int id) {
        switch (id) {
            case 1: return new TestClass1();

            case 2: return new TestClass2();

            case 3: return new TestClass3();

            case 4: return new TestClass4();

            case 5: return new TestClass5();

            case 6: return new TestClass6();

            case 7: return new TestClass7();

            case 8: return new TestClass8();

            case 9: return new TestClass9();

            case 10: return new TestClass10();
        }

        fail();

        return null;
    }

    /**
     *
     */
    static class TestClass1 implements Serializable {}

    /**
     *
     */
    static class TestClass2 implements Serializable {}

    /**
     *
     */
    static class TestClass3 implements Serializable {}

    /**
     *
     */
    static class TestClass4 implements Serializable {}

    /**
     *
     */
    static class TestClass5 implements Serializable {}

    /**
     *
     */
    static class TestClass6 implements Serializable {}

    /**
     *
     */
    static class TestClass7 implements Serializable {}

    /**
     *
     */
    static class TestClass8 implements Serializable {}

    /**
     *
     */
    static class TestClass9 implements Serializable {}

    /**
     *
     */
    static class TestClass10 implements Serializable {}
}
