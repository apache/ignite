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
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.testframework.junits.common.*;

import javax.cache.*;
import java.io.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;

/**
 * Cache future self test.
 */
public class CacheFutureExceptionSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setGridName(gridName);

        if (gridName.equals(getTestGridName(1)))
            cfg.setClientMode(true);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testAsyncCacheFuture() throws Exception {
        Ignite srv = startGrid(0);

        IgniteCache<String, NotSerializableClass> cache = srv.getOrCreateCache("CACHE");
        cache.put("key", new NotSerializableClass());

        Ignite client = startGrid(1);

        IgniteCache<String, NotSerializableClass> asyncCache = client.<String, NotSerializableClass>cache("CACHE").withAsync();

        System.setProperty("FAIL", "true");

        asyncCache.get("key");

        final CountDownLatch futLatch = new CountDownLatch(1);

        asyncCache.future().listen(new IgniteInClosure<IgniteFuture<Object>>() {
            @Override public void apply(IgniteFuture<Object> fut) {
                assertTrue(fut.isDone());

                try {
                    fut.get();

                    fail();
                }
                catch (CacheException e) {
                    log.info("Expected error: " + e);

                    futLatch.countDown();
                }
            }
        });

        assertTrue(futLatch.await(60, SECONDS));
    }

    /**
     * Test class.
     */
    private static class NotSerializableClass implements Serializable {
        /** {@inheritDoc}*/
        private void writeObject(ObjectOutputStream out) throws IOException {
            out.writeObject(this);
        }

        /** {@inheritDoc}*/
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            if (System.getProperty("FAIL") != null)
                throw new RuntimeException("Deserialization failed.");

            in.readObject();
        }
    }
}	