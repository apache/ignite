/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.binary;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.junit.Test;

/**
 *
 */
public class BinaryTxCacheLocalEntriesSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final String FIELD = "user-name";

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(igniteInstanceName);

        ccfg.setStoreKeepBinary(true);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setMarshaller(new BinaryMarshaller());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalEntries() throws Exception {
        IgniteCache<Integer, BinaryObject> cache = grid(0).cache(DEFAULT_CACHE_NAME).withKeepBinary();

        final int ENTRY_CNT = 10;

        for (int i = 0; i < ENTRY_CNT; i++)
            cache.put(i, userObject("user-" + i));

        assertEquals(ENTRY_CNT, cache.localSize(CachePeekMode.ALL));

        for (int i = 0; i < gridCount(); i++)
            jcache(i).withKeepBinary().localEntries();

        cache.removeAll();
    }

    /**
     * @param userName User name.
     * @return Binary object.
     */
    private BinaryObject userObject(String userName) {
        return grid(0).binary().builder("orders").setField(FIELD, userName).build();
    }
}
