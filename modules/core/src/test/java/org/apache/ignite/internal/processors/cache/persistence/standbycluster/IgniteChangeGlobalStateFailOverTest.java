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

package org.apache.ignite.internal.processors.cache.persistence.standbycluster;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.junit.Test;

import static java.lang.Thread.sleep;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 *
 */
public class IgniteChangeGlobalStateFailOverTest extends IgniteChangeGlobalStateAbstractTest {
    /** {@inheritDoc} */
    @Override protected int primaryNodes() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override protected int primaryClientNodes() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override protected int backUpClientNodes() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override protected int backUpNodes() {
        return 4;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateDeActivateOnFixTopology() throws Exception {
        final Ignite igB1 = backUp(0);
        final Ignite igB2 = backUp(1);
        final Ignite igB3 = backUp(2);

        assertTrue(!igB1.active());
        assertTrue(!igB2.active());
        assertTrue(!igB3.active());

        final AtomicInteger cntA = new AtomicInteger();
        final AtomicInteger cntD = new AtomicInteger();

        final AtomicLong timeA = new AtomicLong();
        final AtomicLong timeD = new AtomicLong();

        final AtomicBoolean stop = new AtomicBoolean();

        final AtomicBoolean canAct = new AtomicBoolean(true);

        try {
            final IgniteInternalFuture<Void> af = runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    while (!stop.get()) {
                        Ignite ig = randomBackUp(false);

                        if (canAct.get()) {
                            long start = System.currentTimeMillis();

                            ig.active(true);

                            timeA.addAndGet((System.currentTimeMillis() - start));

                            cntA.incrementAndGet();

                            for (Ignite ign : allBackUpNodes())
                                assertTrue(ign.active());

                            canAct.set(false);
                        }

                    }
                    return null;
                }
            });

            final IgniteInternalFuture<Void> df = runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    while (!stop.get()) {
                        Ignite ig = randomBackUp(false);

                        if (!canAct.get()) {
                            long start = System.currentTimeMillis();

                            ig.active(false);

                            timeD.addAndGet((System.currentTimeMillis() - start));

                            cntD.incrementAndGet();

                            for (Ignite ign : allBackUpNodes())
                                assertTrue(!ign.active());

                            canAct.set(true);
                        }

                    }
                    return null;
                }
            });

            sleep(30_000);

            stop.set(true);

            af.get();
            df.get();
        }
        finally {
            log.info("total activate/deactivate:" + cntA.get() + "/" + cntD.get() + " aTime/dTime:"
                + (timeA.get() / cntA.get() + "/" + (timeD.get() / cntD.get()) + " nodes: " + backUpNodes()));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateDeActivateOnJoiningNode() throws Exception {
        final Ignite igB1 = backUp(0);
        final Ignite igB2 = backUp(1);
        final Ignite igB3 = backUp(2);

        assertTrue(!igB1.active());
        assertTrue(!igB2.active());
        assertTrue(!igB3.active());

        final AtomicInteger cntA = new AtomicInteger();
        final AtomicInteger cntD = new AtomicInteger();

        final AtomicLong timeA = new AtomicLong();
        final AtomicLong timeD = new AtomicLong();

        final AtomicBoolean stop = new AtomicBoolean();

        final AtomicInteger seqIdx = new AtomicInteger(backUpNodes());

        final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

        try {
            final IgniteInternalFuture<Void> af = runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    while (!stop.get()) {
                        rwLock.readLock().lock();

                        try {
                            Ignite ig = randomBackUp(false);

                            long start = System.currentTimeMillis();

                            ig.active(true);

                            timeA.addAndGet((System.currentTimeMillis() - start));

                            cntA.incrementAndGet();

                            for (Ignite ign : allBackUpNodes())
                                assertTrue(ign.active());
                        }
                        finally {
                            rwLock.readLock().unlock();
                        }
                    }

                    return null;
                }
            });

            final IgniteInternalFuture<Void> df = runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    while (!stop.get()) {
                        rwLock.writeLock().lock();

                        try {
                            Ignite ig = randomBackUp(false);

                            long start = System.currentTimeMillis();

                            ig.active(false);

                            timeD.addAndGet((System.currentTimeMillis() - start));

                            cntD.incrementAndGet();

                            for (Ignite ign : allBackUpNodes())
                                assertTrue(!ign.active());
                        }
                        finally {
                            rwLock.writeLock().unlock();
                        }
                    }

                    return null;
                }
            });

            IgniteInternalFuture<Void> jf1 = runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    while (!stop.get()) {
                        rwLock.readLock().lock();

                        try {
                            startBackUp(seqIdx.incrementAndGet());
                        }
                        finally {
                            rwLock.readLock().unlock();
                        }
                    }

                    return null;
                }
            });

            IgniteInternalFuture<Void> jf2 = runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    while (!stop.get()) {
                        rwLock.readLock().lock();

                        try {
                            startBackUp(seqIdx.incrementAndGet());
                        }
                        finally {
                            rwLock.readLock().unlock();
                        }
                    }

                    return null;
                }
            });

            sleep(30_000);

            stop.set(true);

            af.get();
            df.get();
            jf1.get();
            jf2.get();
        }
        finally {
            log.info("Total started nodes: " + (seqIdx.get() - backUpNodes()));

            log.info("Total activate/deactivate:" + cntA.get() + "/" + cntD.get() + " aTime/dTime: "
                + (timeA.get() / cntA.get() + "/" + (timeD.get() / cntD.get()))
            );
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testActivateDeActivateOnFixTopologyWithPutValues() throws Exception {
        final Ignite igB1 = backUp(0);
        final Ignite igB2 = backUp(1);
        final Ignite igB3 = backUp(2);

        assertTrue(!igB1.active());
        assertTrue(!igB2.active());
        assertTrue(!igB3.active());

        final CacheConfiguration<String, String> ccfg = new CacheConfiguration<>();
        ccfg.setName("main-cache");

        final AtomicInteger cnt = new AtomicInteger();

        final AtomicInteger cntA = new AtomicInteger();
        final AtomicInteger cntD = new AtomicInteger();

        final AtomicLong timeA = new AtomicLong();
        final AtomicLong timeD = new AtomicLong();

        final AtomicBoolean stop = new AtomicBoolean();

        final AtomicBoolean canAct = new AtomicBoolean(true);

        try {
            final IgniteInternalFuture<Void> af = runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    while (!stop.get()) {
                        Ignite ig = randomBackUp(false);

                        if (canAct.get()) {
                            long start = System.currentTimeMillis();

                            ig.active(true);

                            IgniteCache<String, String> cache = ig.getOrCreateCache(ccfg);

                            cache.put("key" + cnt.get(), "value" + cnt.get());

                            cnt.incrementAndGet();

                            timeA.addAndGet((System.currentTimeMillis() - start));

                            cntA.incrementAndGet();

                            for (Ignite ign : allBackUpNodes())
                                assertTrue(ign.active());

                            canAct.set(false);
                        }

                    }
                    return null;
                }
            });

            final IgniteInternalFuture<Void> df = runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    while (!stop.get()) {
                        Ignite ig = randomBackUp(false);

                        if (!canAct.get()) {
                            long start = System.currentTimeMillis();

                            IgniteCache<String, String> cache = ig.getOrCreateCache(ccfg);

                            for (int i = 0; i < cnt.get(); i++)
                                assertEquals("value" + i, cache.get("key" + i));

                            ig.active(false);

                            timeD.addAndGet((System.currentTimeMillis() - start));

                            cntD.incrementAndGet();

                            for (Ignite ign : allBackUpNodes())
                                assertTrue(!ign.active());

                            canAct.set(true);
                        }

                    }
                    return null;
                }
            });

            sleep(30_000);

            stop.set(true);

            af.get();
            df.get();
        }
        finally {
            log.info("Total activate/deactivate:" + cntA.get() + "/" + cntD.get() + " aTime/dTime:"
                + (timeA.get() / cntA.get() + "/" + (timeD.get() / cntD.get()) + " nodes: " + backUpNodes()));
        }
    }
}
