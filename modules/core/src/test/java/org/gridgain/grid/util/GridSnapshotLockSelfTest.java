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

package org.gridgain.grid.util;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 *
 */
public class GridSnapshotLockSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testSyncConsistent() throws Exception {
        final AtomicBoolean stop = new AtomicBoolean();

        final AtomicLong x = new AtomicLong();
        final AtomicLong y = new AtomicLong();
        final AtomicLong z = new AtomicLong();

        final Random rnd = new Random();

        final String oops = "Oops!";

        final GridSnapshotLock<T3<Long, Long, Long>> lock = new GridSnapshotLock<T3<Long, Long, Long>>() {
            @Override protected T3<Long, Long, Long> doSnapshot() {
                if (rnd.nextBoolean())
                    throw new IgniteException(oops);

                return new T3<>(x.get(), y.get(), z.get());
            }
        };

        IgniteFuture<?> fut1 = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Random rnd = new Random();

                while(!stop.get()) {
                    if (rnd.nextBoolean()) {
                        if (!lock.tryBeginUpdate())
                            continue;
                    }
                    else
                        lock.beginUpdate();

                    int n = 1 + rnd.nextInt(1000);

                    if (rnd.nextBoolean())
                        x.addAndGet(n);
                    else
                        y.addAndGet(n);

                    z.addAndGet(n);

                    lock.endUpdate();
                }

                return null;
            }
        }, 15, "update");

        IgniteFuture<?> fut2 = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                while(!stop.get()) {
                    T3<Long, Long, Long> t;

                    try {
                        t = lock.snapshot();
                    }
                    catch (IgniteException e) {
                        assertEquals(oops, e.getMessage());

                        continue;
                    }

                    assertEquals(t.get3().longValue(), t.get1() + t.get2());
                }

                return null;
            }
        }, 8, "snapshot");

        Thread.sleep(20000);

        stop.set(true);

        fut1.get();
        fut2.get();
    }
}
