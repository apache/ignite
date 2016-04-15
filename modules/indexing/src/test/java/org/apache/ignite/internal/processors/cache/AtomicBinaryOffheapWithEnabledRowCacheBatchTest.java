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
import org.apache.ignite.configuration.*;

import javax.cache.processor.*;

/**
 * The test are checking batch operation onto atomic offheap cache with per certain key and value types.
 */
public class AtomicBinaryOffheapWithEnabledRowCacheBatchTest extends AtomicBinaryOffheapBatchTest {
    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration configuration = super.cacheConfiguration(gridName);

        configuration.setSqlOnheapRowCacheSize(1);

        return configuration;
    }

    /**
     * Test method.
     *
     * @throws Exception If fail.
     */
    @Override public void testBatchOperations() throws Exception {
        Ignite ignite = ignite(0);
        try (IgniteCache<Object, Object> dfltCache = ignite.cache(null)) {

            try (IgniteDataStreamer<Object, Object> dataLdr = ignite.dataStreamer(dfltCache.getName())) {
                for (int id = 0; id < 50; id++)
                    dataLdr.addData(id, new Organization(id, "Organization " + id));
            }

            for ( int i = 0; i < 50; i++ ) {
                dfltCache.invoke(i, new CacheEntryProcessor<Object, Object, Object>() {
                    @Override
                    public Object process(MutableEntry<Object, Object> entry,
                        Object... arguments) throws EntryProcessorException {
                        entry.remove();
                        return null;
                    }
                });
            }
        }
    }
}
