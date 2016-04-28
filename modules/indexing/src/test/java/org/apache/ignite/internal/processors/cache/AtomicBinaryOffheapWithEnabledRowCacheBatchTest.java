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

import java.util.List;
import org.apache.ignite.*;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.*;

import javax.cache.processor.*;
import org.apache.ignite.marshaller.Marshaller;

/**
 * The test are checking batch operation onto atomic offheap cache with per certain key and value types.
 */
public class AtomicBinaryOffheapWithEnabledRowCacheBatchTest extends AtomicBinaryOffheapBatchTest {
    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setCacheMode(CacheMode.LOCAL);

        cfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);

        cfg.setSqlOnheapRowCacheSize(1);

        cfg.setIndexedTypes(Integer.class, Organization.class);

        return cfg;
    }

    /**
     * Test method.
     *
     * @throws Exception If fail.
     */
    @Override public void testBatchOperations() throws Exception {
        Ignite ignite = ignite(0);

        try (IgniteCache<Object, Object> dfltCache = ignite.cache(null)) {
            int total = 1000;

            for (int id = 0; id < total; id++)
                dfltCache.put(id, new Organization(id, "Organization " + id));

            dfltCache.invoke(0, new CacheEntryProcessor<Object, Object, Object>() {
                @Override
                public Object process(MutableEntry<Object, Object> entry,
                    Object... arguments) throws EntryProcessorException {

                    entry.remove();

                    return null;
                }
            });

            QueryCursor<List<?>> query = dfltCache.query(new SqlFieldsQuery("select _key,_val from Organization where id=0"));
            assertEquals(0, query.getAll().size());

            query = dfltCache.query(new SqlFieldsQuery("select _key,_val from Organization where id=1"));
            assertEquals(1, query.getAll().size());

            assertEquals(total - 1, dfltCache.size());
        }
    }
}
