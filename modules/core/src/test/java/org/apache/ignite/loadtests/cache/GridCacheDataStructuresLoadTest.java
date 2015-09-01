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

package org.apache.ignite.loadtests.cache;

import java.util.Arrays;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Cache data structures load test.
 */
public final class GridCacheDataStructuresLoadTest extends GridCacheAbstractLoadTest {
    /** Atomic long name. */
    private static final String TEST_LONG_NAME = "test-atomic-long";

    /** Atomic reference name. */
    private static final String TEST_REF_NAME = "test-atomic-ref";

    /** Atomic sequence name. */
    private static final String TEST_SEQ_NAME = "test-atomic-seq";

    /** Atomic stamped name. */
    private static final String TEST_STAMP_NAME = "test-atomic-stamp";

    /** Queue name. */
    private static final String TEST_QUEUE_NAME = "test-queue";

    /** Count down latch name. */
    private static final String TEST_LATCH_NAME = "test-latch";

    /** */
    private static final CollectionConfiguration colCfg = new CollectionConfiguration();

    /** Maximum added value. */
    private static final int MAX_INT = 1000;

    /** Count down latch initial count. */
    private static final int LATCH_INIT_CNT = 1000;

    /** */
    private static final boolean LONG = false;

    /** */
    private static final boolean REF = false;

    /** */
    private static final boolean SEQ = false;

    /** */
    private static final boolean STAMP = false;

    /** */
    private static final boolean QUEUE = false;

    /** */
    private static final boolean LATCH = true;

    /** */
    private GridCacheDataStructuresLoadTest() {
        // No-op
    }

    /** Atomic long write closure. */
    private final CIX1<Ignite> longWriteClos =
        new CIX1<Ignite>() {
        @Override public void applyx(Ignite ignite) {
            IgniteAtomicLong al = ignite.atomicLong(TEST_LONG_NAME, 0, true);

            for (int i = 0; i < operationsPerTx; i++) {
                al.addAndGet(RAND.nextInt(MAX_INT));

                long cnt = writes.incrementAndGet();

                if (cnt % WRITE_LOG_MOD == 0)
                    info("Performed " + cnt + " writes.");
            }
        }
    };

    /** Atomic long read closure. */
    private final CIX1<Ignite> longReadClos =
        new CIX1<Ignite>() {
        @Override public void applyx(Ignite ignite) {
            IgniteAtomicLong al = ignite.atomicLong(TEST_LONG_NAME, 0, true);

            for (int i = 0; i < operationsPerTx; i++) {
                al.get();

                long cnt = reads.incrementAndGet();

                if (cnt % READ_LOG_MOD == 0)
                    info("Performed " + cnt + " reads.");
            }
        }
    };

    /** Atomic reference write closure. */
    private final CIX1<Ignite> refWriteClos =
        new CIX1<Ignite>() {
        @Override public void applyx(Ignite ignite) {
            IgniteAtomicReference<Integer> ar = ignite.atomicReference(TEST_REF_NAME,
                null, true);

            for (int i = 0; i < operationsPerTx; i++) {
                ar.set(RAND.nextInt(MAX_INT));

                long cnt = writes.incrementAndGet();

                if (cnt % WRITE_LOG_MOD == 0)
                    info("Performed " + cnt + " writes.");
            }
        }
    };

    /** Atomic reference read closure. */
    private final CIX1<Ignite> refReadClos =
        new CIX1<Ignite>() {
        @Override public void applyx(Ignite ignite) {
            IgniteAtomicReference<Integer> ar = ignite.atomicReference(TEST_REF_NAME, null,
                true);

            for (int i = 0; i < operationsPerTx; i++) {
                ar.get();

                long cnt = reads.incrementAndGet();

                if (cnt % READ_LOG_MOD == 0)
                    info("Performed " + cnt + " reads.");
            }
        }
    };

    /** Atomic sequence write closure. */
    private final CIX1<Ignite> seqWriteClos =
        new CIX1<Ignite>() {
        @Override public void applyx(Ignite ignite) {
            IgniteAtomicSequence as = ignite.atomicSequence(TEST_SEQ_NAME, 0, true);

            for (int i = 0; i < operationsPerTx; i++) {
                as.addAndGet(RAND.nextInt(MAX_INT) + 1);

                long cnt = writes.incrementAndGet();

                if (cnt % WRITE_LOG_MOD == 0)
                    info("Performed " + cnt + " writes.");
            }
        }
    };

    /** Atomic sequence read closure. */
    private final CIX1<Ignite> seqReadClos =
        new CIX1<Ignite>() {
        @Override public void applyx(Ignite ignite) {
            IgniteAtomicSequence as = ignite.atomicSequence(TEST_SEQ_NAME, 0, true);

            for (int i = 0; i < operationsPerTx; i++) {
                as.get();

                long cnt = reads.incrementAndGet();

                if (cnt % READ_LOG_MOD == 0)
                    info("Performed " + cnt + " reads.");
            }
        }
    };

    /** Atomic stamped write closure. */
    private final CIX1<Ignite> stampWriteClos =
        new CIX1<Ignite>() {
        @Override public void applyx(Ignite ignite) {
            IgniteAtomicStamped<Integer, Integer> as = ignite.atomicStamped(TEST_STAMP_NAME,
                0, 0, true);

            for (int i = 0; i < operationsPerTx; i++) {
                as.set(RAND.nextInt(MAX_INT), RAND.nextInt(MAX_INT));

                long cnt = writes.incrementAndGet();

                if (cnt % WRITE_LOG_MOD == 0)
                    info("Performed " + cnt + " writes.");
            }
        }
    };

    /** Atomic stamped read closure. */
    private final CIX1<Ignite> stampReadClos =
        new CIX1<Ignite>() {
        @Override public void applyx(Ignite ignite) {
            IgniteAtomicStamped<Integer, Integer> as = ignite.atomicStamped(TEST_STAMP_NAME,
                0, 0, true);

            for (int i = 0; i < operationsPerTx; i++) {
                as.get();

                long cnt = reads.incrementAndGet();

                if (cnt % READ_LOG_MOD == 0)
                    info("Performed " + cnt + " reads.");
            }
        }
    };

    /** Queue write closure. */
    private final CIX1<Ignite> queueWriteClos =
        new CIX1<Ignite>() {
        @Override public void applyx(Ignite ignite) {
            IgniteQueue<Integer> q = ignite.queue(TEST_QUEUE_NAME, 0, colCfg);

            for (int i = 0; i < operationsPerTx; i++) {
                q.put(RAND.nextInt(MAX_INT));

                long cnt = writes.incrementAndGet();

                if (cnt % WRITE_LOG_MOD == 0)
                    info("Performed " + cnt + " writes.");
            }
        }
    };

    /** Queue read closure. */
    private final CIX1<Ignite> queueReadClos =
        new CIX1<Ignite>() {
        @Override public void applyx(Ignite ignite) {
            IgniteQueue<Integer> q = ignite.queue(TEST_QUEUE_NAME, 0, colCfg);

            for (int i = 0; i < operationsPerTx; i++) {
                q.peek();

                long cnt = reads.incrementAndGet();

                if (cnt % READ_LOG_MOD == 0)
                    info("Performed " + cnt + " reads.");
            }
        }
    };

    /** Count down latch write closure. */
    private final CIX1<Ignite> latchWriteClos =
        new CIX1<Ignite>() {
        @Override public void applyx(Ignite ignite) {
            IgniteCountDownLatch l = ignite.countDownLatch(TEST_LATCH_NAME, LATCH_INIT_CNT, true, true);

            for (int i = 0; i < operationsPerTx; i++) {
                l.countDown();

                long cnt = writes.incrementAndGet();

                if (cnt % WRITE_LOG_MOD == 0)
                    info("Performed " + cnt + " writes.");
            }
        }
    };

    /** Count down latch read closure. */
    private final CIX1<Ignite> latchReadClos =
        new CIX1<Ignite>() {
        @Override public void applyx(Ignite ignite) {
            IgniteCountDownLatch l = ignite.countDownLatch(TEST_LATCH_NAME, LATCH_INIT_CNT, true, true);

            for (int i = 0; i < operationsPerTx; i++) {
                l.count();

                long cnt = reads.incrementAndGet();

                if (cnt % READ_LOG_MOD == 0)
                    info("Performed " + cnt + " reads.");
            }
        }
    };

    /**
     * @param args Arguments.
     * @throws IgniteCheckedException In case of error.
     */
    public static void main(String[] args) throws IgniteCheckedException {
        System.setProperty(IgniteSystemProperties.IGNITE_UPDATE_NOTIFIER, "false");

        System.out.println("Starting master node [params=" + Arrays.toString(args) + ']');

        String cfg = args.length >= 1 ? args[0] : CONFIG_FILE;
        String log = args.length >= 2 ? args[1] : LOG_FILE;

        final GridCacheDataStructuresLoadTest test = new GridCacheDataStructuresLoadTest();

        try (Ignite g = Ignition.start(test.configuration(cfg, log))) {
            System.gc();

            if (LONG) {
                info("Testing atomic long...");

                test.loadTestIgnite(test.longWriteClos, test.longReadClos);
            }

            System.gc();

            if (REF) {
                info("Testing atomic reference...");

                test.loadTestIgnite(test.refWriteClos, test.refReadClos);
            }

            System.gc();

            if (SEQ) {
                info("Testing atomic sequence...");

                test.loadTestIgnite(test.seqWriteClos, test.seqReadClos);
            }

            System.gc();

            if (STAMP) {
                info("Testing atomic stamped...");

                test.loadTestIgnite(test.stampWriteClos, test.stampReadClos);
            }

            System.gc();

            if (QUEUE) {
                info("Testing queue...");

                test.loadTestIgnite(test.queueWriteClos, test.queueReadClos);
            }

            System.gc();

            if (LATCH) {
                info("Testing count down latch...");

                test.loadTestIgnite(test.latchWriteClos, test.latchReadClos);
            }
        }
    }

    /**
     * @param writeClos Write closure.
     * @param readClos ReadClosure.
     */
    protected void loadTestIgnite(final CIX1<Ignite> writeClos, final CIX1<Ignite> readClos) {
        info("Read threads: " + readThreads());
        info("Write threads: " + writeThreads());
        info("Test duration (ms): " + testDuration);

        final Ignite ignite = G.ignite();

        final IgniteCache<Integer, Integer> cache = ignite.cache(null);

        assert cache != null;

        try {
            IgniteInternalFuture<?> f1 = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    long start = System.currentTimeMillis();

                    while (!done.get()) {
                        if (tx) {
                            try (Transaction tx = ignite.transactions().txStart()) {
                                writeClos.apply(ignite);

                                tx.commit();
                            }
                        }
                        else
                            writeClos.apply(ignite);
                    }

                    writeTime.addAndGet(System.currentTimeMillis() - start);

                    return null;
                }
            }, writeThreads(), "cache-load-test-worker");

            IgniteInternalFuture<?> f2 = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    long start = System.currentTimeMillis();

                    while(!done.get()) {
                        if (tx) {
                            try (Transaction tx = ignite.transactions().txStart()) {
                                readClos.apply(ignite);

                                tx.commit();
                            }
                        }
                        else
                            readClos.apply(ignite);
                    }

                    readTime.addAndGet(System.currentTimeMillis() - start);

                    return null;
                }
            }, readThreads(), "cache-load-test-worker");

            Thread.sleep(testDuration);

            done.set(true);

            f1.get();
            f2.get();

            info("Test stats: ");
            info("    total-threads = " + threads);
            info("    write-ratio = " + writeRatio);
            info("    total-runs = " + (reads.get() + writes.get()));
            info("    total-reads = " + reads);
            info("    total-writes = " + writes);
            info("    read-time (ms) = " + readTime);
            info("    write-time (ms) = " + writeTime);
            info("    avg-read-time (ms) = " + ((double)readTime.get() / reads.get()));
            info("    avg-write-time (ms) = " + ((double)writeTime.get() / writes.get()));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}