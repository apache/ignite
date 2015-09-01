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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.cache.CacheException;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedFullApiSelfTest;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.CLOCK;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;

/**
 * Multi node test for disabled near cache.
 */
public class GridCacheAtomicFullApiSelfTest extends GridCachePartitionedFullApiSelfTest {
    /**
     * @return Write order mode for atomic cache.
     */
    protected CacheAtomicWriteOrderMode atomicWriteOrderMode() {
        return CLOCK;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected boolean txEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected boolean lockingEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setAtomicWriteOrderMode(atomicWriteOrderMode());

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLock() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return jcache().lock("1").tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            }
        }, CacheException.class, "Locks are not supported");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return jcache().lockAll(Collections.singleton("1")).tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            }
        }, CacheException.class, "Locks are not supported");
    }

    /**
     * @throws Exception In case of error.
     */
    @Override public void testGetAll() throws Exception {
        jcache().put("key1", 1);
        jcache().put("key2", 2);

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                jcache().getAll(null).isEmpty();

                return null;
            }
        }, NullPointerException.class, null);

        assert jcache().getAll(Collections.<String>emptySet()).isEmpty();

        Map<String, Integer> map1 = jcache().getAll(ImmutableSet.of("key1", "key2", "key9999"));

        info("Retrieved map1: " + map1);

        assert 2 == map1.size() : "Invalid map: " + map1;

        assertEquals(1, (int)map1.get("key1"));
        assertEquals(2, (int)map1.get("key2"));
        assertNull(map1.get("key9999"));

        Map<String, Integer> map2 = jcache().getAll(ImmutableSet.of("key1", "key2", "key9999"));

        info("Retrieved map2: " + map2);

        assert 2 == map2.size() : "Invalid map: " + map2;

        assertEquals(1, (int)map2.get("key1"));
        assertEquals(2, (int)map2.get("key2"));
        assertNull(map2.get("key9999"));
    }
}