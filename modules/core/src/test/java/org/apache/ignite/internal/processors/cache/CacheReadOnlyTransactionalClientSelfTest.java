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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.processor.*;

/**
 * Tests for read-only transactional cache client.
 */
public class CacheReadOnlyTransactionalClientSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** */
    private boolean nearEnabled;

    /** */
    private Factory<CacheStore> factory;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClientMode(client);

        CacheConfiguration cc = new CacheConfiguration();

        cc.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cc.setCacheStoreFactory(factory);

        if (client && nearEnabled)
            cc.setNearConfiguration(new NearCacheConfiguration());

        cfg.setCacheConfiguration(cc);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        client = false;
        factory = new Factory1();

        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopGrid();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCorrectStore() throws Exception {
        client = true;
        nearEnabled = false;
        factory = new Factory1();

        Ignite ignite = startGrid();

        IgniteCache cache = ignite.cache(null);

        cache.get(0);
        cache.getAll(F.asSet(0, 1));
        cache.getAndPut(0, 0);
        cache.getAndPutIfAbsent(0, 0);
        cache.getAndRemove(0);
        cache.getAndReplace(0, 0);
        cache.put(0, 0);
        cache.putAll(F.asMap(0, 0, 1, 1));
        cache.putIfAbsent(0, 0);
        cache.remove(0);
        cache.remove(0, 0);
        cache.removeAll(F.asSet(0, 1));
        cache.removeAll();
        cache.invoke(0, new EP());
        cache.invokeAll(F.asSet(0, 1), new EP());
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvalidStore() throws Exception {
        client = true;
        nearEnabled = false;
        factory = new Factory2();

        try {
            startGrid();

            assert false : "Exception was not thrown.";
        }
        catch (Exception e) {
            assert e.getMessage().startsWith("Store factory mismatch") : e.getMessage();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoStoreNearDisabled() throws Exception {
        nearEnabled = false;

        doTestNoStore();
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoStoreNearEnabled() throws Exception {
        nearEnabled = true;

        doTestNoStore();
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestNoStore() throws Exception {
        client = true;
        factory = null;

        Ignite ignite = startGrid();

        IgniteCache cache = ignite.cache(null);

        cache.get(0);
        cache.getAll(F.asSet(0, 1));

        try {
            cache.getAndPut(0, 0);
        }
        catch (CacheException e) {
            assertEquals("Updates are not allowed for cache: null", e.getMessage());
        }

        try {
            cache.getAndPutIfAbsent(0, 0);
        }
        catch (CacheException e) {
            assertEquals("Updates are not allowed for cache: null", e.getMessage());
        }

        try {
            cache.getAndRemove(0);
        }
        catch (CacheException e) {
            assertEquals("Updates are not allowed for cache: null", e.getMessage());
        }

        try {
            cache.getAndReplace(0, 0);
        }
        catch (CacheException e) {
            assertEquals("Updates are not allowed for cache: null", e.getMessage());
        }

        try {
            cache.put(0, 0);
        }
        catch (CacheException e) {
            assertEquals("Updates are not allowed for cache: null", e.getMessage());
        }

        try {
            cache.putAll(F.asMap(0, 0, 1, 1));
        }
        catch (CacheException e) {
            assertEquals("Updates are not allowed for cache: null", e.getMessage());
        }

        try {
            cache.putIfAbsent(0, 0);
        }
        catch (CacheException e) {
            assertEquals("Updates are not allowed for cache: null", e.getMessage());
        }

        try {
            cache.remove(0);
        }
        catch (CacheException e) {
            assertEquals("Updates are not allowed for cache: null", e.getMessage());
        }

        try {
            cache.remove(0, 0);
        }
        catch (CacheException e) {
            assertEquals("Updates are not allowed for cache: null", e.getMessage());
        }

        try {
            cache.removeAll(F.asSet(0, 1));
        }
        catch (CacheException e) {
            assertEquals("Updates are not allowed for cache: null", e.getMessage());
        }

        try {
            cache.removeAll();
        }
        catch (CacheException e) {
            assertEquals("Updates are not allowed for cache: null", e.getMessage());
        }

        try {
            cache.invoke(0, new EP());
        }
        catch (CacheException e) {
            assertEquals("Updates are not allowed for cache: null", e.getMessage());
        }

        try {
            cache.invokeAll(F.asSet(0, 1), new EP());
        }
        catch (CacheException e) {
            assertEquals("Updates are not allowed for cache: null", e.getMessage());
        }
    }

    /**
     */
    private static class Factory1 implements Factory<CacheStore> {
        /** {@inheritDoc} */
        @Override public CacheStore create() {
            return null;
        }
    }

    /**
     */
    private static class Factory2 implements Factory<CacheStore> {
        /** {@inheritDoc} */
        @Override public CacheStore create() {
            return null;
        }
    }

    /**
     */
    private static class EP implements CacheEntryProcessor {
        @Override public Object process(MutableEntry entry, Object... arguments) {
            return null;
        }
    }
}
