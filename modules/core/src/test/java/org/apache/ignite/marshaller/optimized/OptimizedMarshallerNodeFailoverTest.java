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
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 *
 */
public class OptimizedMarshallerNodeFailoverTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean cache;

    /** */
    private String workDir;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setMarshaller(new OptimizedMarshaller());

        cfg.setWorkDirectory(workDir);

        if (cache) {
            CacheConfiguration ccfg = new CacheConfiguration();

            ccfg.setCacheMode(PARTITIONED);
            ccfg.setBackups(1);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);

            cfg.setCacheConfiguration(ccfg);
        }
        else
            cfg.setClientMode(true);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassCacheUpdateFailover1() throws Exception {
        classCacheUpdateFailover(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassCacheUpdateFailover2() throws Exception {
        classCacheUpdateFailover(true);
    }

    /**
     * @param stopSrv If {@code true} restarts server node, otherwise client node.
     * @throws Exception If failed.
     */
    private void classCacheUpdateFailover(boolean stopSrv) throws Exception {
        cache = true;

        startGridsMultiThreaded(2);

        cache = stopSrv;

        IgniteCache<Integer, Object> cache0 = ignite(0).cache(null);

        for (int i = 0; i < 20; i++) {
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

            cache0.putAll(map);

            fut.get();
        }

        cache = true;

        Ignite ignite = startGrid(2); // Check can start one more cache node.

        assertNotNull(ignite.cache(null));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRestartAllNodes() throws Exception {
        cache = true;

        String home = U.getIgniteHome();

        String[] workDirs = new String[3];

        for (int i = 0; i < 3; i++) {
            workDirs[i] = home + "/work/marshallerTestNode_" + i;

            File file = new File(workDirs[i]);

            if (file.exists())
                assert U.delete(file);
        }

        try {
            for (int i = 0; i < workDirs.length; i++) {
                workDir = workDirs[i];

                U.nullifyWorkDirectory();

                startGrid(i);
            }

            Marshaller marsh = ignite(0).configuration().getMarshaller();

            TestClass1 obj = new TestClass1();

            obj.val = 111;

            byte[] bytes = marsh.marshal(obj);

            stopAllGrids();

            for (int i = 0; i < workDirs.length; i++) {
                workDir = workDirs[i];

                U.nullifyWorkDirectory();

                startGrid(i);
            }

            for (int i = 0; i < 3; i++) {
                marsh = ignite(i).configuration().getMarshaller();

                TestClass1 obj0 = marsh.unmarshal(bytes, null);

                assertEquals(111, obj0.val);
            }
        }
        finally {
            for (String dir : workDirs)
                assert U.delete(new File(dir));
        }
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

            case 11: return new TestClass11();

            case 12: return new TestClass12();

            case 13: return new TestClass13();

            case 14: return new TestClass14();

            case 15: return new TestClass15();

            case 16: return new TestClass16();

            case 17: return new TestClass17();

            case 18: return new TestClass18();

            case 19: return new TestClass19();

            case 20: return new TestClass20();
        }

        fail();

        return null;
    }

    /**
     *
     */
    static class TestClass1 implements Serializable {
        /** */
        int val;
    }

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

    /**
     *
     */
    static class TestClass11 implements Serializable {}

    /**
     *
     */
    static class TestClass12 implements Serializable {}

    /**
     *
     */
    static class TestClass13 implements Serializable {}

    /**
     *
     */
    static class TestClass14 implements Serializable {}

    /**
     *
     */
    static class TestClass15 implements Serializable {}

    /**
     *
     */
    static class TestClass16 implements Serializable {}

    /**
     *
     */
    static class TestClass17 implements Serializable {}

    /**
     *
     */
    static class TestClass18 implements Serializable {}

    /**
     *
     */
    static class TestClass19 implements Serializable {}

    /**
     *
     */
    static class TestClass20 implements Serializable {}
}
