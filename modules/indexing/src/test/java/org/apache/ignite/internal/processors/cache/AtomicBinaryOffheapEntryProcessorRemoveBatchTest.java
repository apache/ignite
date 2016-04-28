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
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;

import javax.cache.processor.*;

/**
 * The test are checking batch operation onto atomic offheap cache with per certain key and value types.
 */
public class AtomicBinaryOffheapEntryProcessorRemoveBatchTest extends AtomicBinaryOffheapBaseBatchTest {
    /** {@inheritDoc} */
    @Override protected int onHeapRowCacheSize() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected Class<?>[] indexedTypes() {
        return new Class<?>[]{Integer.class, Organization.class};
    }

    /**
     * Test method.
     *
     * @throws Exception If fail.
     */
    @Override public void testBatchOperations() throws Exception {
        fail("IGNITE-2982");

        Ignite ignite = ignite(0);

        int iterations = 50;

        while (iterations-- >= 0) {
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

                QueryCursor<List<?>> q = dfltCache.query(new SqlFieldsQuery("select _key,_val from Organization where id=0"));

                assertEquals(0, q.getAll().size());

                q = dfltCache.query(new SqlFieldsQuery("select _key,_val from Organization where id=1"));

                assertEquals(1, q.getAll().size());

                assertEquals(total - 1, dfltCache.size());
            }
        }
    }
}