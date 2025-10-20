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

package org.apache.ignite.internal.processors.cache.persistence.snapshot.dump;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.dump.DumpEntry;
import org.apache.ignite.dump.DumpReader;
import org.apache.ignite.dump.DumpReaderConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheType;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.AbstractCacheDumpTest.TestDumpConsumer;
import org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.configuration.AtomicConfiguration.DFLT_ATOMIC_SEQUENCE_RESERVE_SIZE;
import static org.apache.ignite.dump.DumpReaderConfiguration.DFLT_THREAD_CNT;
import static org.apache.ignite.dump.DumpReaderConfiguration.DFLT_TIMEOUT;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.AbstractCacheDumpTest.DMP_NAME;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/** */
@RunWith(Parameterized.class)
public class IgniteCacheDumpDataStructuresTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameter
    public int nodes;

    /** */
    @Parameterized.Parameter(1)
    public int backups;

    /** */
    @Parameterized.Parameter(2)
    public CacheAtomicityMode amode;

    /** */
    @Parameterized.Parameter(3)
    public CacheMode cmode;

    /** */
    @Parameterized.Parameter(4)
    @Nullable public String grp;

    /** */
    @Parameterized.Parameters(name = "nodes={0},backups={1},amode={2},cmode={3},grp={4}")
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();

        for (int nodes : new int[]{1, 3})
            for (int backups : nodes == 1 ? new int[] {0} : new int[]{0, 2})
                for (CacheAtomicityMode amode : CacheAtomicityMode.values()) {
                    for (CacheMode cmode : CacheMode.values()) {
                        for (String grp : new String[]{null, "mygroup"})
                            params.add(new Object[]{nodes, backups, amode, cmode, grp});
                    }
                }

        return params;
    }

    /** */
    private static final String qname = "TEST-QUEUE";

    /** */
    private static final int alinc = 42;

    /** */
    private static final int qcap = 21;

    /** */
    private static final String sname = "TEST-SET";

    /** */
    private static final String COUNT_DOWN_LATCH = "COUNT-DOWN-LATCH";

    /** */
    private static final String SEMAPHORE_NAME = "SEMAPHORE";

    /** */
    private static final String LOCK_NAME = "LOCK";

    /** */
    private static final String ATOMIC_LONG = "ATOMIC-LONG";

    /** */
    private static final String ATOMIC_REF = "ATOMIC-REF";

    /** */
    private static final String REFVALUE = "value";

    /** */
    private static final String ATOMIC_SEQ = "ATOMIC-SEQ";

    /** */
    private static final String ATOMIC_STAMPED = "ATOMIC-STAMPED";

    /** */
    private static final String STAMP_VAL = "stamp-val";

    /** */
    private static final String STAMP = "stamp";

    /** */
    private static final List<String> QUEUE_CONTENT = Arrays.asList("one", "two", "three");

    /** */
    private static final List<Integer> SET_CONTENT = Arrays.asList(1, 2, 3);

    /** */
    private CountDownLatch latch;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** Checks a dump when it is created just after a restart. */
    @Test
    public void testRestoreDataStructuresNoBackgroundChanges() throws Exception {
        doTestRestoreDataStructure(() -> latch.countDown(), (ign, ccfg) -> new RestoreDataStructureConsumer(ign, ccfg) {
            /** {@inheritDoc} */
            @Override public void check() {
                super.check();

                IgniteQueue<String> q = ign.queue(qname, qcap, ccfg);

                assertNotNull(q);
                assertEquals(QUEUE_CONTENT.size(), q.size());

                for (String expEl : QUEUE_CONTENT)
                    assertEquals(expEl, q.remove());

                IgniteSet<Integer> s = ign.set(sname, ccfg);

                assertNotNull(s);
                assertEquals(SET_CONTENT.size(), s.size());
                assertTrue(s.containsAll(SET_CONTENT));

                assertNull("CountDownLatch volatile, must not be restored", ign.countDownLatch(COUNT_DOWN_LATCH, 10, true, false));
                assertNull("Semaphore volatile, must not be restored", ign.semaphore(SEMAPHORE_NAME, 10, true, false));
                assertNull("ReentrantLock volatile, must not be restored", ign.reentrantLock(LOCK_NAME, true, true, false));

                IgniteAtomicLong al = ign.atomicLong(ATOMIC_LONG, 0, false);

                assertNotNull(al);
                assertEquals(alinc, al.get());
                assertEquals(alinc + 1, al.incrementAndGet());

                assertEquals("value", ign.atomicReference(ATOMIC_REF, null, false).get());

                IgniteAtomicSequence as = ign.atomicSequence(ATOMIC_SEQ, 0, false);

                assertNotNull(as);
                assertEquals(DFLT_ATOMIC_SEQUENCE_RESERVE_SIZE, as.get());
                assertEquals(DFLT_ATOMIC_SEQUENCE_RESERVE_SIZE + 1, as.incrementAndGet());

                IgniteAtomicStamped<Object, Object> ast = ign.atomicStamped(ATOMIC_STAMPED, null, null, false);

                assertNotNull(ast);
                assertEquals(STAMP_VAL, ast.value());
                assertEquals(STAMP, ast.stamp());

                assertNull("Volatile DS not dumped", ign.semaphore(SEMAPHORE_NAME, 2, true, false));
                assertNull("Volatile DS not dumped", ign.countDownLatch(COUNT_DOWN_LATCH, 10, true, false));
                assertNull("Volatile DS not dumped", ign.reentrantLock(LOCK_NAME, true, true, false));
            }
        });
    }

    /** Checks a dump when it is created just after a restart. */
    @Test
    public void testRestoreDataStructuresWithBackgroundChanges() throws Exception {
        AtomicLong atomicLongVal = new AtomicLong();
        AtomicLong atomicSeqVal = new AtomicLong();

        doTestRestoreDataStructure(() -> {
            List<IgniteAtomicLong> atomicLongs = new ArrayList<>(nodes);
            List<IgniteAtomicSequence> atomicSequences = new ArrayList<>(nodes);

            for (int i = 0; i < nodes; i++) {
                IgniteEx ign = grid(i);

                IgniteAtomicLong al = ign.atomicLong(ATOMIC_LONG, 0, false);
                IgniteAtomicSequence as = ign.atomicSequence(ATOMIC_SEQ, 0, false);

                assertNotNull(al);
                assertNotNull(as);

                atomicLongVal.set(Math.max(atomicLongVal.get(), al.get()));
                atomicSeqVal.set(Math.max(atomicSeqVal.get(), as.get()));

                atomicLongs.add(al);
                atomicSequences.add(as);
            }

            latch.countDown();

            while (!Thread.interrupted()) {
                for (int i = 0; i < nodes; i++) {
                    atomicLongVal.set(Math.max(atomicLongVal.get(), atomicLongs.get(i).incrementAndGet()));
                    atomicSeqVal.set(Math.max(atomicSeqVal.get(), atomicSequences.get(i).incrementAndGet()));
                }
            }
        }, (ign, ccfg) -> new RestoreDataStructureConsumer(ign, ccfg) {
            @Override public void check() {
                super.check();

                IgniteAtomicLong al = ign.atomicLong(ATOMIC_LONG, 0, false);

                assertNotNull(al);
                assertTrue("Atomic cache values saved on each increment", al.get() <= atomicLongVal.get());

                IgniteAtomicSequence as = ign.atomicSequence(ATOMIC_SEQ, 0, false);

                assertNotNull(as);
                as.incrementAndGet();

                assertNull("Volatile DS not dumped", ign.semaphore(SEMAPHORE_NAME, 2, true, false));
                assertNull("Volatile DS not dumped", ign.countDownLatch(COUNT_DOWN_LATCH, 10, true, false));
                assertNull("Volatile DS not dumped", ign.reentrantLock(LOCK_NAME, true, true, false));
            }
        });
    }

    /** */
    private void doTestRestoreDataStructure(
        Runnable backgroundChanger,
        BiFunction<IgniteEx, CollectionConfiguration, TestDumpConsumer> cnsmrFactory
    ) throws Exception {
        IgniteEx ign = startGrids(nodes);

        ign.cluster().state(ClusterState.ACTIVE);

        CollectionConfiguration ccfg = new CollectionConfiguration()
            .setBackups(backups)
            .setAtomicityMode(amode)
            .setCacheMode(cmode)
            .setGroupName(grp);

        ign.queue(qname, qcap, ccfg).addAll(QUEUE_CONTENT);
        ign.set(sname, ccfg).addAll(SET_CONTENT);
        ign.atomicLong(ATOMIC_LONG, 0, true).getAndAdd(alinc);
        ign.atomicReference(ATOMIC_REF, REFVALUE, true);
        ign.atomicSequence(ATOMIC_SEQ, 0, true).incrementAndGet();
        ign.atomicStamped(ATOMIC_STAMPED, STAMP_VAL, STAMP, true);

        ign.semaphore(SEMAPHORE_NAME, 1, true, true).acquire();
        ign.reentrantLock(LOCK_NAME, true, true, true).lock();
        ign.countDownLatch(COUNT_DOWN_LATCH, 10, true, true).countDown(3);

        latch = new CountDownLatch(1);

        IgniteInternalFuture<Object> fut = runAsync(backgroundChanger);

        assertTrue(latch.await(60, TimeUnit.SECONDS));

        IgniteSnapshotManager snpMgr = ign.context().cache().context().snapshotMgr();

        snpMgr.createSnapshot(DMP_NAME, null, null, false, false, true, false, false, true).get(getTestTimeout());

        fut.cancel();

        File dumpDir = snapshotFileTree(ign, DMP_NAME).root();

        stopAllGrids();

        cleanPersistenceDir(true);

        IgniteEx ign1 = startGrids(nodes);

        ign1.cluster().state(ClusterState.ACTIVE);

        TestDumpConsumer cnsmr = cnsmrFactory.apply(ign1, ccfg);

        new DumpReader(
            new DumpReaderConfiguration(
                null,
                dumpDir.getAbsolutePath(),
                null,
                cnsmr,
                DFLT_THREAD_CNT,
                DFLT_TIMEOUT,
                true,
                false,
                false,
                null,
                null,
                false,
                null
            ),
            log
        ).run();

        cnsmr.check();
    }

    /** */
    private abstract static class RestoreDataStructureConsumer extends TestDumpConsumer {
        /** */
        protected final IgniteEx ign;

        /** */
        protected final CollectionConfiguration ccfg;

        /** */
        public RestoreDataStructureConsumer(IgniteEx ign, CollectionConfiguration ccfg) {
            this.ign = ign;
            this.ccfg = ccfg;
        }

        /** {@inheritDoc} */
        @Override public void onMappings(Iterator<TypeMapping> mappings) {
            super.onMappings(mappings);
        }

        /** {@inheritDoc} */
        @Override public void onTypes(Iterator<BinaryType> types) {
            super.onTypes(types);
        }

        /** {@inheritDoc} */
        @Override public void onCacheConfigs(Iterator<StoredCacheData> caches) {
            super.onCacheConfigs(caches);

            caches.forEachRemaining(data -> {
                if (DataStructuresProcessor.isDataStructureCache(data.config().getName())) {
                    CacheConfiguration<?, ?> ccfg = data.config();

                    try {
                        ign.context().cache().dynamicStartCache(ccfg,
                            ccfg.getName(),
                            null,
                            CacheType.DATA_STRUCTURES,
                            false,
                            false,
                            false,
                            true).get();
                    }
                    catch (IgniteCheckedException e) {
                        throw new RuntimeException(e);
                    }
                }
                else {
                    ign.createCache(data.config());
                }
            });
        }

        /** {@inheritDoc} */
        @Override public void onPartition(int grp, int part, Iterator<DumpEntry> data) {
            CacheGroupContext gctx = ign.context().cache().cacheGroup(grp);

            assertNotNull(gctx);
            assertEquals(gctx.cacheType(), CacheType.DATA_STRUCTURES);

            data.forEachRemaining(e -> {
                GridCacheContext<?, ?> cctx =
                    gctx.caches().stream().filter(c -> c.cacheId() == e.cacheId()).findFirst().get();

                IgniteInternalCache<Object, Object> cache = ign.context().cache().cache(cctx.name());

                try {
                    cache.put(e.key(), e.value());
                }
                catch (IgniteCheckedException ex) {
                    throw new RuntimeException(ex);
                }
            });
        }
    }
}
