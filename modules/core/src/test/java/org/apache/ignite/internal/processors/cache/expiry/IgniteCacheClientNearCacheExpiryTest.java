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

package org.apache.ignite.internal.processors.cache.expiry;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import javax.cache.expiry.*;

import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;

/**
 *
 */
public class IgniteCacheClientNearCacheExpiryTest extends IgniteCacheAbstractTest {
    /** */
    private static final int NODES = 3;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return NODES;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return new NearCacheConfiguration();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (gridName.equals(getTestGridName(NODES - 1)))
            cfg.setClientMode(true);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testExpirationOnClient() throws Exception {
        Ignite ignite = grid(NODES - 1);

        assertTrue(ignite.configuration().isClientMode());

        IgniteCache<Object, Object> cache = ignite.cache(null);

        assertTrue(((IgniteCacheProxy)cache).context().isNear());

        for (int i = 0 ; i < 100; i++)
            cache.put(i, i);

        CreatedExpiryPolicy plc = new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, 500));

        IgniteCache<Object, Object> cacheWithExpiry = cache.withExpiryPolicy(plc);

        for (int i = 100 ; i < 200; i++) {
            cacheWithExpiry.put(i, i);

            assertEquals(i, cacheWithExpiry.localPeek(i));
        }

        U.sleep(1000);

        for (int i = 0 ; i < 100; i++)
            assertEquals(i, cacheWithExpiry.localPeek(i));

        for (int i = 100 ; i < 200; i++)
            assertNull(cache.localPeek(i));
    }
}
