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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.log4j.*;
import org.apache.ignite.internal.util.typedef.*;
import org.gridgain.testframework.*;

import javax.cache.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.GridCacheMode.*;

/**
 * Test cases for multi-threaded tests.
 */
public class GridCachePartitionedLockSelfTest extends GridCacheLockAbstractTest {
    /** */
    private static final boolean CACHE_DEBUG = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        if (CACHE_DEBUG)
            resetLog4j(Level.DEBUG, true, GridCacheProcessor.class.getPackage().getName());

        return super.getConfiguration(gridName);
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected boolean isPartitioned() {
        return true;
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testLockAtomicCache() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName(getTestGridName(0));
        cfg.setClientConnectionConfiguration(null);
        cfg.setCacheConfiguration(new CacheConfiguration());

        final Ignite g0 = G.start(cfg);

        final IgniteCache<Object, Object> cache = g0.jcache(null);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return cache.lock(1).tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            }
        }, CacheException.class, "Locks are not supported");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return cache.lockAll(Collections.singleton(1)).tryLock(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            }
        }, CacheException.class, "Locks are not supported");
    }
}
