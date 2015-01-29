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
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;

import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheMode.*;

/**
 * Test for asynchronous cache entry lock with timeout.
 */
public class GridCachePartitionedEntryLockSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("BusyWait")
    public void testLockAsyncWithTimeout() throws Exception {
        cache().put("key", 1);

        for (int i = 0; i < gridCount(); i++) {
            final CacheEntry<String, Integer> e = cache(i).entry("key");

            if (e.backup()) {
                assert !e.isLocked();

                e.lockAsync(2000).get();

                assert e.isLocked();

                IgniteCompute comp = compute(grid(i).forLocal()).withAsync();

                comp.call(new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        IgniteInternalFuture<Boolean> f = e.lockAsync(1000);

                        try {
                            f.get(100);

                            fail();
                        }
                        catch (IgniteFutureTimeoutCheckedException ex) {
                            info("Caught expected exception: " + ex);
                        }

                        try {
                            assert f.get();
                        }
                        finally {
                            e.unlock();
                        }

                        return true;
                    }
                });

                IgniteInternalFuture<Boolean> f = comp.future();

                // Let another thread start.
                Thread.sleep(300);

                assert e.isLocked();
                assert e.isLockedByThread();

                cache().unlock("key");

                assert f.get();

                for (int j = 0; j < 100; j++)
                    if (cache().isLocked("key") || cache().isLockedByThread("key"))
                        Thread.sleep(10);
                    else
                        break;

                assert !cache().isLocked("key");
                assert !cache().isLockedByThread("key");

                break;
            }
        }
    }
}
