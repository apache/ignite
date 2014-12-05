/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.spring;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;
import org.springframework.beans.factory.*;
import org.springframework.context.support.*;

/**
 * Spring cache test.
 */
public class GridSpringCacheManagerSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME = "testCache";

    /** */
    private GridSpringCacheTestService svc;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration cache = new GridCacheConfiguration();

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
        BeanFactory factory = new ClassPathXmlApplicationContext("org/gridgain/grid/cache/spring/spring-caching.xml");

        svc = (GridSpringCacheTestService)factory.getBean("testService");

        svc.reset();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid().cache(CACHE_NAME).removeAll();
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

        GridCache<Integer, String> c = grid().cache(CACHE_NAME);

        assertEquals(3, c.size());

        for (int i = 0; i < 3; i++)
            assertEquals("value" + i, c.get(i));
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

        GridCache<GridSpringCacheTestKey, String> c = grid().cache(CACHE_NAME);

        assertEquals(3, c.size());

        for (int i = 0; i < 3; i++)
            assertEquals("value" + i + "suffix" + i, c.get(new GridSpringCacheTestKey(i, "suffix" + i)));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleKeyPut() throws Exception {
        GridCache<Integer, String> c = grid().cache(CACHE_NAME);

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
    public void testComplexKeyPut() throws Exception {
        GridCache<GridSpringCacheTestKey, String> c = grid().cache(CACHE_NAME);

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
    public void testSimpleKeyEvict() throws Exception {
        GridCache<Integer, String> c = grid().cache(CACHE_NAME);

        for (int i = 0; i < 3; i++)
            c.putx(i, "value" + i);

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
        GridCache<GridSpringCacheTestKey, String> c = grid().cache(CACHE_NAME);

        for (int i = 0; i < 3; i++)
            c.putx(new GridSpringCacheTestKey(i, "suffix" + i), "value" + i);

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
        GridCache<Integer, String> c = grid().cache(CACHE_NAME);

        for (int i = 0; i < 3; i++)
            c.putx(i, "value" + i);

        assertEquals(3, c.size());

        assertEquals("value0", c.get(0));
        assertEquals("value1", c.get(1));
        assertEquals("value2", c.get(2));

        svc.evictAll();

        assertEquals(0, c.size());
    }
}
