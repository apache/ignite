/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.handlers.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.kernal.processors.rest.handlers.*;
import org.gridgain.grid.kernal.processors.rest.request.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.testframework.junits.common.*;

import java.lang.reflect.*;
import java.util.*;

/**
 * Tests command handler directly.
 */
public class GridCacheCommandHandlerSelfTest extends GridCommonAbstractTest {
    /**
     * Constructor.
     */
    public GridCacheCommandHandlerSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration() throws Exception {
        // Discovery config.
        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(new GridTcpDiscoveryVmIpFinder(true));

        // Cache config.
        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(GridCacheMode.LOCAL);
        cacheCfg.setQueryIndexEnabled(false);

        // Grid config.
        GridConfiguration cfg = super.getConfiguration();

        cfg.setLocalHost("localhost");
        cfg.setRestEnabled(true);
        cfg.setDiscoverySpi(disco);
        cfg.setCacheConfiguration(cacheCfg); // Add 'null' cache configuration.

        return cfg;
    }

    /**
     * Tests the cache failure during the execution of the CACHE_GET command.
     *
     * @throws Exception If failed.
     */
    public void testCacheGetFailsSyncNotify() throws Exception {
        GridRestCommandHandler hnd = new TestableGridCacheCommandHandler(((GridKernal)grid()).context(), "getAsync",
            true);

        GridRestCacheRequest req = new GridRestCacheRequest();

        req.command(GridRestCommand.CACHE_GET);

        req.key("k1");

        try {
            hnd.handleAsync(req).get();

            fail("Expected exception not thrown.");
        }
        catch (GridException e) {
            info("Got expected exception: " + e);
        }
    }

    /**
     * Tests the cache failure during the execution of the CACHE_GET command.
     *
     * @throws Exception If failed.
     */
    public void testCacheGetFailsAsyncNotify() throws Exception {
        GridRestCommandHandler hnd = new TestableGridCacheCommandHandler(((GridKernal)grid()).context(), "getAsync",
            false);

        GridRestCacheRequest req = new GridRestCacheRequest();

        req.command(GridRestCommand.CACHE_GET);

        req.key("k1");

        try {
            hnd.handleAsync(req).get();

            fail("Expected exception not thrown.");
        }
        catch (GridException e) {
            info("Got expected exception: " + e);
        }
    }

    /**
     * Test cache handler append/prepend commands.
     *
     * @throws Exception In case of any exception.
     */
    @SuppressWarnings("NullableProblems")
    public void testAppendPrepend() throws Exception {
        assertEquals("as" + "df", testAppend("as", "df", true));
        assertEquals("df" + "as", testAppend("as", "df", false));

        List<String> curList = new ArrayList<>(Arrays.asList("a", "b"));
        List<String> newList = new ArrayList<>(Arrays.asList("b", "c"));

        assertEquals(Arrays.asList("a", "b", "b", "c"), testAppend(curList, newList, true));
        assertEquals(Arrays.asList("b", "c", "a", "b"), testAppend(curList, newList, false));

        Set<String> curSet = new HashSet<>(Arrays.asList("a", "b"));
        Set<String> newSet = new HashSet<>(Arrays.asList("b", "c"));
        Set<String> resSet = new HashSet<>(Arrays.asList("a", "b", "c"));

        assertEquals(resSet, testAppend(curSet, newSet, true));
        assertEquals(resSet, testAppend(curSet, newSet, false));
        assertEquals(resSet, testAppend(newSet, curList, true));
        assertEquals(resSet, testAppend(newSet, curList, false));
        assertEquals(resSet, testAppend(curSet, newList, true));
        assertEquals(resSet, testAppend(curSet, newList, false));

        Map<String, String> curMap = F.asMap("a", "1", "b", "2", "c", "3");
        Map<String, String> newMap = F.asMap("a", "#", "b", null, "c", "%", "d", "4");

        assertEquals(F.asMap("a", "#", "c", "%", "d", "4"), testAppend(curMap, newMap, true));
        assertEquals(F.asMap("a", "1", "b", "2", "c", "3", "d", "4"), testAppend(curMap, newMap, false));

        try {
            testAppend("as", Arrays.asList("df"), true);

            fail("Expects failed with incompatible types message.");
        }
        catch (GridException e) {
            info("Got expected exception: " + e);

            assertTrue(e.getMessage().startsWith("Incompatible types"));
        }
    }

    /**
     * Test cache handler append/prepend commands with specified environment.
     *
     * @param curVal Current value in cache.
     * @param newVal New value to append/prepend.
     * @param append Append or prepend flag.
     * @param <T> Cache value type.
     * @return Resulting value in cache.
     * @throws GridException In case of any grid exception.
     */
    private <T> T testAppend(T curVal, T newVal, boolean append) throws GridException {
        GridRestCommandHandler hnd = new GridCacheCommandHandler(((GridKernal)grid()).context());

        String key = UUID.randomUUID().toString();

        GridRestCacheRequest req = new GridRestCacheRequest();

        req.command(append ? GridRestCommand.CACHE_APPEND : GridRestCommand.CACHE_PREPEND);

        req.key(key);
        req.value(newVal);

        assertFalse("Expects failure due to no value in cache.", (Boolean)hnd.handleAsync(req).get().getResponse());

        T res;

        try {
            // Change cache state.
            cache().putx(key, curVal);

            // Validate behavior for initialized cache (has current value).
            assertTrue("Expects succeed.", (Boolean)hnd.handleAsync(req).get().getResponse());
        }
        finally {
            res = (T)cache().remove(key);
        }

        return res;
    }

    /**
     * Test command handler.
     */
    private static class TestableGridCacheCommandHandler extends GridCacheCommandHandler {
        /** */
        private final String failMtd;

        /** */
        private final boolean sync;

        /**
         * Constructor.
         *
         * @param ctx Context.
         * @param failMtd Method to fail.
         * @param sync Sync notification flag.
         */
        TestableGridCacheCommandHandler(final GridKernalContext ctx, final String failMtd, final boolean sync) {
            super(ctx);

            this.failMtd = failMtd;
            this.sync = sync;
        }

        /**
         * @param cacheName Name of the cache.
         *
         * @return Instance of a GridCache proxy.
         */
        @Override protected GridCacheProjectionEx<Object, Object> localCache(String cacheName) throws GridException {
            final GridCacheProjectionEx<Object, Object> cache = super.localCache(cacheName);

            return (GridCacheProjectionEx<Object, Object>)Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class[] {GridCacheProjectionEx.class},
                new InvocationHandler() {
                    @Override public Object invoke(Object proxy, Method mtd, Object[] args) throws Throwable {
                        if (failMtd.equals(mtd.getName())) {
                            GridFuture<Object> fut = new GridFinishedFuture<>(ctx,
                                new GridException("Operation failed"));

                            fut.syncNotify(sync);

                            return fut;
                        }
                        // Rewriting flagsOn result to keep intercepting invocations after it.
                        else if ("flagsOn".equals(mtd.getName()))
                            return proxy;
                        else if ("forSubjectId".equals(mtd.getName()))
                            return proxy;

                        return mtd.invoke(cache, args);
                    }
                });
        }
    }
}
