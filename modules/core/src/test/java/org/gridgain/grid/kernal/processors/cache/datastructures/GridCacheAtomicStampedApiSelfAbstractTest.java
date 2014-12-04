/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

/**
 * Basic tests for atomic stamped.
 */
public abstract class GridCacheAtomicStampedApiSelfAbstractTest extends GridCommonAbstractTest {
    /** */
    protected static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /**
     * Constructs a test.
     */
    protected GridCacheAtomicStampedApiSelfAbstractTest() {
        super(true /* start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        GridTcpDiscoverySpi spi = new GridTcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPrepareAtomicStamped() throws Exception {
        /** Name of first atomic. */
        String atomicName1 = UUID.randomUUID().toString();

        String initVal = "1";
        String initStamp = "2";

        GridCacheAtomicStamped<String, String> atomic1 = grid().cache(null).dataStructures()
            .atomicStamped(atomicName1, initVal, initStamp, true);
        GridCacheAtomicStamped<String, String> atomic2 = grid().cache(null).dataStructures()
            .atomicStamped(atomicName1, null, null, true);

        assertNotNull(atomic1);
        assertNotNull(atomic2);
        assert atomic1.equals(atomic2);
        assert atomic2.equals(atomic1);

        assert initVal.equals(atomic2.value());
        assert initStamp.equals(atomic2.stamp());

        assert grid().cache(null).dataStructures().removeAtomicStamped(atomicName1);
        assert !grid().cache(null).dataStructures().removeAtomicStamped(atomicName1);

        try {
            atomic1.get();
            fail();
        }
        catch (GridException e) {
            info("Caught expected exception: " + e.getMessage());
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testSetAndGet() throws Exception {
        String atomicName = UUID.randomUUID().toString();

        String initVal = "qwerty";
        String initStamp = "asdfgh";

        GridCacheAtomicStamped<String, String> atomic = grid().cache(null).dataStructures()
            .atomicStamped(atomicName, initVal, initStamp, true);

        assertEquals(initVal, atomic.value());
        assertEquals(initStamp, atomic.stamp());
        assertEquals(initVal, atomic.get().get1());
        assertEquals(initStamp, atomic.get().get2());

        atomic.set(null, null);

        assertEquals(null, atomic.value());
        assertEquals(null, atomic.stamp());
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testCompareAndSetSimpleValue() throws Exception {
        String atomicName = UUID.randomUUID().toString();

        String initVal = "qwerty";
        String initStamp = "asdfgh";

        GridCacheAtomicStamped<String, String> atomic = grid().cache(null).dataStructures()
            .atomicStamped(atomicName, initVal, initStamp, true);

        assertEquals(initVal, atomic.value());
        assertEquals(initStamp, atomic.stamp());
        assertEquals(initVal, atomic.get().get1());
        assertEquals(initStamp, atomic.get().get2());

        atomic.compareAndSet("a", "b", "c", "d");

        assertEquals(initVal, atomic.value());
        assertEquals(initStamp, atomic.stamp());

        atomic.compareAndSet(initVal, null, initStamp, null);

        assertEquals(null, atomic.value());
        assertEquals(null, atomic.stamp());
    }
}
