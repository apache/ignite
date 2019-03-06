/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */
package org.apache.ignite.internal.processors.cache.eviction.paged;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 *
 */
public class PageEvictionDataStreamerTest extends PageEvictionMultinodeAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        return setEvictionMode(DataPageEvictionMode.RANDOM_LRU, super.getConfiguration(gridName));
    }

    /** {@inheritDoc} */
    @Override protected void createCacheAndTestEviction(CacheConfiguration<Object, Object> cfg) throws Exception {
        IgniteCache<Object, Object> cache = clientGrid().getOrCreateCache(cfg);

        try (IgniteDataStreamer<Object, Object> ldr = clientGrid().dataStreamer(cfg.getName())) {
            ldr.allowOverwrite(true);

            for (int i = 1; i <= ENTRIES; i++) {
                ThreadLocalRandom r = ThreadLocalRandom.current();

                if (r.nextInt() % 5 == 0)
                    ldr.addData(i, new TestObject(PAGE_SIZE / 4 - 50 + r.nextInt(5000))); // Fragmented object.
                else
                    ldr.addData(i, new TestObject(r.nextInt(PAGE_SIZE / 4 - 50))); // Fits in one page.

                if (i % (ENTRIES / 10) == 0)
                    System.out.println(">>> Entries put: " + i);
            }
        }

        int resultingSize = cache.size(CachePeekMode.PRIMARY);

        System.out.println(">>> Resulting size: " + resultingSize);

        // Eviction started, no OutOfMemory occurred, success.
        assertTrue(resultingSize < ENTRIES);

        clientGrid().destroyCache(cfg.getName());
    }
}
