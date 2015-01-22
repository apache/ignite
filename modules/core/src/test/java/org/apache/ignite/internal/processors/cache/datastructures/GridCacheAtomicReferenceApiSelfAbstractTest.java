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

package org.apache.ignite.internal.processors.cache.datastructures;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.datastructures.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;
import org.mockito.*;

import javax.cache.*;
import javax.cache.configuration.*;
import javax.cache.integration.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.GridCacheAtomicityMode.*;
import static org.apache.ignite.cache.GridCacheDistributionMode.*;

/**
 * Basic tests for atomic reference.
 */
public abstract class GridCacheAtomicReferenceApiSelfAbstractTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private AtomicBoolean storeCalled = new AtomicBoolean();

    /**
     * Constructs a test.
     */
    protected GridCacheAtomicReferenceApiSelfAbstractTest() {
        super(true /* start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        return cfg;
    }

    /**
     * @return Cache configuration for the test.
     */
    @SuppressWarnings("unchecked")
    protected CacheConfiguration getCacheConfiguration() {
        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setDistributionMode(NEAR_PARTITIONED);

        ccfg.setCacheStoreFactory(new FactoryBuilder.SingletonFactory(new CacheStoreAdapter() {
            @Override public Object load(Object key) throws CacheLoaderException {
                storeCalled.set(true);

                return null;
            }

            @Override public void write(Cache.Entry entry) throws CacheWriterException {
                storeCalled.set(true);
            }

            @Override public void delete(Object key) throws CacheWriterException {
                storeCalled.set(true);
            }
        }));

        ccfg.setReadThrough(true);
        ccfg.setWriteThrough(true);
        ccfg.setLoadPreviousValue(true);

        return ccfg;
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
    public void testPrepareAtomicReference() throws Exception {
        /** Name of first atomic. */
        String atomicName1 = UUID.randomUUID().toString();

        /** Name of second atomic. */
        String atomicName2 = UUID.randomUUID().toString();

        String initVal = "1";
        GridCacheAtomicReference<String> atomic1 = grid().cache(null).dataStructures()
            .atomicReference(atomicName1, initVal, true);
        GridCacheAtomicReference<String> atomic2 = grid().cache(null).dataStructures()
            .atomicReference(atomicName2, null, true);

        assertNotNull(atomic1);
        assertNotNull(atomic2);

        assert grid().cache(null).dataStructures().removeAtomicReference(atomicName1);
        assert grid().cache(null).dataStructures().removeAtomicReference(atomicName2);
        assert !grid().cache(null).dataStructures().removeAtomicReference(atomicName1);
        assert !grid().cache(null).dataStructures().removeAtomicReference(atomicName2);

        try {
            atomic1.get();
            fail();
        }
        catch (IgniteCheckedException e) {
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

        GridCacheAtomicReference<String> atomic = grid().cache(null).dataStructures()
            .atomicReference(atomicName, initVal, true);

        assertEquals(initVal, atomic.get());

        atomic.set(null);

        assertEquals(null, atomic.get());
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testCompareAndSetSimpleValue() throws Exception {
        String atomicName = UUID.randomUUID().toString();

        String initVal = "qwerty";

        GridCacheAtomicReference<String> atomic = grid().cache(null).dataStructures()
            .atomicReference(atomicName, initVal, true);

        assertEquals(initVal, atomic.get());

        atomic.compareAndSet("h", "j");

        assertEquals(initVal, atomic.get());

        atomic.compareAndSet(initVal, null);

        assertEquals(null, atomic.get());
    }

    /**
     * Tests that non-persistent atomic reference doesn't ever
     * hit the store.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void testNonPersistentMode() throws IgniteCheckedException {
        String atomicName = UUID.randomUUID().toString();

        GridCache<Object, Object> cache = grid().cache(null);

        assertNotNull(cache);

        GridCacheAtomicReference<Boolean> atomic = cache.dataStructures().atomicReference(atomicName, false, true);

        atomic.set(true);

        cache.dataStructures().removeAtomicReference(atomicName);

        assertFalse(storeCalled.get());
    }
}
