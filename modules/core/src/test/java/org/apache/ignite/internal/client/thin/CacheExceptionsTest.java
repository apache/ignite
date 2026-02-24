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

package org.apache.ignite.internal.client.thin;

import java.util.Objects;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Thin client cache exceptions tests.
 */
public class CacheExceptionsTest extends AbstractThinClientTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /**
     * Tests cache name in wrapped cache exception for server errors.
     */
    @Test
    public void testCacheExceptionWrapped() {
        try (IgniteClient client = startClient(0)) {
            String cacheName = "testCacheName";

            ClientCache<Object, Objects> cache = client.cache(cacheName);

            // Affinity call.
            GridTestUtils.assertThrowsAnyCause(log, () -> cache.get(0), ClientException.class, cacheName);

            // Async affinity call.
            GridTestUtils.assertThrowsAnyCause(log, () -> cache.getAsync(0).get(), ClientException.class, cacheName);

            // Non-affinity call.
            GridTestUtils.assertThrowsAnyCause(log, () -> cache.size(), ClientException.class, cacheName);

            // Async non-affinity call.
            GridTestUtils.assertThrowsAnyCause(log, () -> cache.sizeAsync().get(), ClientException.class, cacheName);

            // Transactional call.
            GridTestUtils.assertThrowsAnyCause(log, () -> {
                try (ClientTransaction ignore = client.transactions().txStart()) {
                    return cache.get(0);
                }
            }, ClientException.class, cacheName);

            // Async transactional call.
            GridTestUtils.assertThrowsAnyCause(log, () -> {
                try (ClientTransaction ignore = client.transactions().txStart()) {
                    return cache.getAsync(0).get();
                }
            }, ClientException.class, cacheName);

            // Query.
            GridTestUtils.assertThrowsAnyCause(log, () -> cache.query(new ScanQuery<>()).getAll(), ClientException.class, cacheName);

            // Affinity query.
            GridTestUtils.assertThrowsAnyCause(log, () -> cache.query(new ScanQuery<>().setPartition(0)).getAll(),
                ClientException.class, cacheName);
        }
    }
}
