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

package org.apache.ignite.util;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.GridSnapshotLock;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class GridSnapshotLockSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
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

        IgniteInternalFuture<?> fut1 = multithreadedAsync(new Callable<Object>() {
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

        IgniteInternalFuture<?> fut2 = multithreadedAsync(new Callable<Object>() {
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
