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

package org.apache.ignite.cache.spring;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Spring cache test.
 */
public class GridSpringCacheManagerSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME = "testCache";

    /** */
    private static final String DYNAMIC_CACHE_NAME = "dynamicCache";

    /** */
    private static final Object NULL;

    /**
     */
    static {
        try {
            NULL = U.field(SpringCache.class, "NULL");
        }
        catch (IgniteCheckedException e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    private GridSpringCacheTestService svc;

    /** */
    private GridSpringDynamicCacheTestService dynamicSvc;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cache = new CacheConfiguration();

        cache.setName(CACHE_NAME);

        cfg.setCacheConfiguration(cache);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override public String getTestGridName() {
        return "testGrid";
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        BeanFactory factory = new ClassPathXmlApplicationContext("org/apache/ignite/cache/spring/spring-caching.xml");

        svc = (GridSpringCacheTestService)factory.getBean("testService");
        dynamicSvc = (GridSpringDynamicCacheTestService)factory.getBean("dynamicTestService");

        svc.reset();
        dynamicSvc.reset();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid().cache(CACHE_NAME).removeAll();

        grid().destroyCache(DYNAMIC_CACHE_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleKey() throws Exception {
        for (int i = 0; i < 3; i++) {
            assertEquals("value" + i, svc.simpleKey(i));
            assertEquals("value" + i, svc.simpleKey(i));
        }

        assertEquals(3, svc.called());

        IgniteCache<Integer, String> c = grid().cache(CACHE_NAME);

        assertEquals(3, c.size());

        for (int i = 0; i < 3; i++)
            assertEquals("value" + i, c.get(i));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleKeyNullValue() throws Exception {
        for (int i = 0; i < 3; i++) {
            assertNull(svc.simpleKeyNullValue(i));
            assertNull(svc.simpleKeyNullValue(i));
        }

        assertEquals(3, svc.called());

        IgniteCache<Integer, String> c = grid().cache(CACHE_NAME);

        assertEquals(3, c.size());

        for (int i = 0; i < 3; i++)
            assertEquals(NULL, c.get(i));
    }

    /**
     * @throws Exception If failed.
     */
    public void testComplexKey() throws Exception {
        for (int i = 0; i < 3; i++) {
            assertEquals("value" + i + "suffix" + i, svc.complexKey(i, "suffix" + i));
            assertEquals("value" + i + "suffix" + i, svc.complexKey(i, "suffix" + i));
        }

        assertEquals(3, svc.called());

        IgniteCache<GridSpringCacheTestKey, String> c = grid().cache(CACHE_NAME);

        assertEquals(3, c.size());

        for (int i = 0; i < 3; i++)
            assertEquals("value" + i + "suffix" + i, c.get(new GridSpringCacheTestKey(i, "suffix" + i)));
    }

    /**
     * @throws Exception If failed.
     */
    public void testComplexKeyNullValue() throws Exception {
        for (int i = 0; i < 3; i++) {
            assertNull(svc.complexKeyNullValue(i, "suffix" + i));
            assertNull(svc.complexKeyNullValue(i, "suffix" + i));
        }

        assertEquals(3, svc.called());

        IgniteCache<GridSpringCacheTestKey, String> c = grid().cache(CACHE_NAME);

        assertEquals(3, c.size());

        for (int i = 0; i < 3; i++)
            assertEquals(NULL, c.get(new GridSpringCacheTestKey(i, "suffix" + i)));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleKeyPut() throws Exception {
        IgniteCache<Integer, String> c = grid().cache(CACHE_NAME);

        for (int i = 0; i < 3; i++) {
            assertEquals("value" + i + "odd", svc.simpleKeyPut(i));

            assertEquals(i + 1, c.size());
            assertEquals("value" + i + "odd", c.get(i));

            assertEquals("value" + i + "even", svc.simpleKeyPut(i));

            assertEquals(i + 1, c.size());
            assertEquals("value" + i + "even", c.get(i));
        }

        assertEquals(6, svc.called());
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleKeyPutNullValue() throws Exception {
        IgniteCache<Integer, String> c = grid().cache(CACHE_NAME);

        for (int i = 0; i < 3; i++) {
            assertNull(svc.simpleKeyPutNullValue(i));

            assertEquals(i + 1, c.size());
            assertEquals(NULL, c.get(i));

            assertNull(svc.simpleKeyPutNullValue(i));

            assertEquals(i + 1, c.size());
            assertEquals(NULL, c.get(i));
        }

        assertEquals(6, svc.called());
    }

    /**
     * @throws Exception If failed.
     */
    public void testComplexKeyPut() throws Exception {
        IgniteCache<GridSpringCacheTestKey, String> c = grid().cache(CACHE_NAME);

        for (int i = 0; i < 3; i++) {
            assertEquals("value" + i + "suffix" + i + "odd", svc.complexKeyPut(i, "suffix" + i));

            assertEquals(i + 1, c.size());
            assertEquals("value" + i + "suffix" + i + "odd", c.get(new GridSpringCacheTestKey(i, "suffix" + i)));

            assertEquals("value" + i + "suffix" + i + "even", svc.complexKeyPut(i, "suffix" + i));

            assertEquals(i + 1, c.size());
            assertEquals("value" + i + "suffix" + i + "even", c.get(new GridSpringCacheTestKey(i, "suffix" + i)));
        }

        assertEquals(6, svc.called());
    }

    /**
     * @throws Exception If failed.
     */
    public void testComplexKeyPutNullValue() throws Exception {
        IgniteCache<GridSpringCacheTestKey, String> c = grid().cache(CACHE_NAME);

        for (int i = 0; i < 3; i++) {
            assertNull(svc.complexKeyPutNullValue(i, "suffix" + i));

            assertEquals(i + 1, c.size());
            assertEquals(NULL, c.get(new GridSpringCacheTestKey(i, "suffix" + i)));

            assertNull(svc.complexKeyPutNullValue(i, "suffix" + i));

            assertEquals(i + 1, c.size());
            assertEquals(NULL, c.get(new GridSpringCacheTestKey(i, "suffix" + i)));
        }

        assertEquals(6, svc.called());
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleKeyEvict() throws Exception {
        IgniteCache<Integer, String> c = grid().cache(CACHE_NAME);

        for (int i = 0; i < 3; i++)
            c.put(i, "value" + i);

        assertEquals(3, c.size());

        assertEquals("value0", c.get(0));
        assertEquals("value1", c.get(1));
        assertEquals("value2", c.get(2));

        svc.simpleKeyEvict(2);

        assertEquals(2, c.size());

        assertEquals("value0", c.get(0));
        assertEquals("value1", c.get(1));
        assertNull(c.get(2));
    }

    /**
     * @throws Exception If failed.
     */
    public void testComplexKeyEvict() throws Exception {
        IgniteCache<GridSpringCacheTestKey, String> c = grid().cache(CACHE_NAME);

        for (int i = 0; i < 3; i++)
            c.put(new GridSpringCacheTestKey(i, "suffix" + i), "value" + i);

        assertEquals(3, c.size());

        assertEquals("value0", c.get(new GridSpringCacheTestKey(0, "suffix" + 0)));
        assertEquals("value1", c.get(new GridSpringCacheTestKey(1, "suffix" + 1)));
        assertEquals("value2", c.get(new GridSpringCacheTestKey(2, "suffix" + 2)));

        svc.complexKeyEvict(2, "suffix" + 2);

        assertEquals(2, c.size());

        assertEquals("value0", c.get(new GridSpringCacheTestKey(0, "suffix" + 0)));
        assertEquals("value1", c.get(new GridSpringCacheTestKey(1, "suffix" + 1)));
        assertNull(c.get(new GridSpringCacheTestKey(2, "suffix" + 2)));
    }

    /**
     * @throws Exception If failed.
     */
    public void testEvictAll() throws Exception {
        IgniteCache<Integer, String> c = grid().cache(CACHE_NAME);

        for (int i = 0; i < 3; i++)
            c.put(i, "value" + i);

        assertEquals(3, c.size());

        assertEquals("value0", c.get(0));
        assertEquals("value1", c.get(1));
        assertEquals("value2", c.get(2));

        svc.evictAll();

        assertEquals(0, c.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testDynamicCache() throws Exception {
        for (int i = 0; i < 3; i++) {
            assertEquals("value" + i, dynamicSvc.cacheable(i));
            assertEquals("value" + i, dynamicSvc.cacheable(i));
        }

        assertEquals(3, dynamicSvc.called());

        IgniteCache<Integer, String> c = grid().cache(DYNAMIC_CACHE_NAME);

        // Check that correct config is used.
        assertEquals(2, c.getConfiguration(CacheConfiguration.class).getBackups());

        assertEquals(3, c.size());

        for (int i = 0; i < 3; i++)
            assertEquals("value" + i, c.get(i));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDynamicCachePut() throws Exception {
        for (int i = 0; i < 3; i++) {
            assertEquals("value" + i, dynamicSvc.cachePut(i));
            assertEquals("value" + i, dynamicSvc.cachePut(i));
        }

        assertEquals(6, dynamicSvc.called());

        IgniteCache<Integer, String> c = grid().cache(DYNAMIC_CACHE_NAME);

        // Check that correct config is used.
        assertEquals(2, c.getConfiguration(CacheConfiguration.class).getBackups());

        assertEquals(3, c.size());

        for (int i = 0; i < 3; i++)
            assertEquals("value" + i, c.get(i));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDynamicCacheEvict() throws Exception {
        CacheConfiguration<Integer, String> cacheCfg = new CacheConfiguration<>();

        cacheCfg.setName(DYNAMIC_CACHE_NAME);

        IgniteCache<Integer, String> c = grid().createCache(cacheCfg);

        for (int i = 0; i < 3; i++)
            c.put(i, "value" + i);

        assertEquals(3, c.size());

        for (int i = 0; i < 2; i++) {
            dynamicSvc.cacheEvict(i);
            dynamicSvc.cacheEvict(i);
        }

        assertEquals(4, dynamicSvc.called());

        assertEquals(1, c.size());

        assertEquals("value2", c.get(2));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDynamicCacheEvictAll() throws Exception {
        CacheConfiguration<Integer, String> cacheCfg = new CacheConfiguration<>();

        cacheCfg.setName(DYNAMIC_CACHE_NAME);

        IgniteCache<Integer, String> c = grid().createCache(cacheCfg);

        for (int i = 0; i < 3; i++)
            c.put(i, "value" + i);

        assertEquals(3, c.size());

        dynamicSvc.cacheEvictAll();
        dynamicSvc.cacheEvictAll();

        assertEquals(2, dynamicSvc.called());

        assertEquals(0, c.size());
    }
}
