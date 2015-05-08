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

package org.apache.ignite.internal.processors.rest.handlers.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.rest.*;
import org.apache.ignite.internal.processors.rest.handlers.*;
import org.apache.ignite.internal.processors.rest.request.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

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
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        // Discovery config.
        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        // Cache config.
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(CacheMode.LOCAL);

        // Grid config.
        IgniteConfiguration cfg = super.getConfiguration();

        cfg.setLocalHost("localhost");

        ConnectorConfiguration clnCfg = new ConnectorConfiguration();
        clnCfg.setHost("localhost");

        cfg.setConnectorConfiguration(clnCfg);
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
        GridRestCommandHandler hnd = new TestableCacheCommandHandler(((IgniteKernal)grid()).context(), "getAsync");

        GridRestCacheRequest req = new GridRestCacheRequest();

        req.command(GridRestCommand.CACHE_GET);

        req.key("k1");

        try {
            hnd.handleAsync(req).get();

            fail("Expected exception not thrown.");
        }
        catch (IgniteCheckedException e) {
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
        catch (IgniteCheckedException e) {
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
     * @throws IgniteCheckedException In case of any grid exception.
     */
    private <T> T testAppend(T curVal, T newVal, boolean append) throws IgniteCheckedException {
        GridRestCommandHandler hnd = new GridCacheCommandHandler(((IgniteKernal)grid()).context());

        String key = UUID.randomUUID().toString();

        GridRestCacheRequest req = new GridRestCacheRequest();

        req.command(append ? GridRestCommand.CACHE_APPEND : GridRestCommand.CACHE_PREPEND);

        req.key(key);
        req.value(newVal);

        assertFalse("Expects failure due to no value in cache.", (Boolean)hnd.handleAsync(req).get().getResponse());

        T res;

        try {
            // Change cache state.
            jcache().put(key, curVal);

            // Validate behavior for initialized cache (has current value).
            assertTrue("Expects succeed.", (Boolean)hnd.handleAsync(req).get().getResponse());
        }
        finally {
            res = (T)jcache().getAndRemove(key);
        }

        return res;
    }

    /**
     * Test command handler.
     */
    private static class TestableCacheCommandHandler extends GridCacheCommandHandler {
        /** */
        private final String failMtd;

        /**
         * Constructor.
         *  @param ctx Context.
         * @param failMtd Method to fail.
         */
        TestableCacheCommandHandler(final GridKernalContext ctx, final String failMtd) {
            super(ctx);

            this.failMtd = failMtd;
        }

        /**
         * @param cacheName Name of the cache.
         *
         * @return Instance of a Cache proxy.
         */
        @Override protected GridCacheProjectionEx<Object, Object> localCache(String cacheName) throws IgniteCheckedException {
            final GridCacheProjectionEx<Object, Object> cache = super.localCache(cacheName);

            return (GridCacheProjectionEx<Object, Object>)Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class[] {GridCacheProjectionEx.class},
                new InvocationHandler() {
                    @Override public Object invoke(Object proxy, Method mtd, Object[] args) throws Throwable {
                        if (failMtd.equals(mtd.getName())) {
                            IgniteInternalFuture<Object> fut = new GridFinishedFuture<>(
                                new IgniteCheckedException("Operation failed"));

                            return fut;
                        }
                        // Rewriting flagOn result to keep intercepting invocations after it.
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
