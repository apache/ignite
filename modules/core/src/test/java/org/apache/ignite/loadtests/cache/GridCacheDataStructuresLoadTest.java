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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.datastructures.*;
import org.apache.ignite.internal.util.typedef.*;

import java.util.*;

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
    private final CIX1<CacheProjection<Integer, Integer>> longWriteClos =
        new CIX1<CacheProjection<Integer, Integer>>() {
        @Override public void applyx(CacheProjection<Integer, Integer> cache)
            throws IgniteCheckedException {
            CacheAtomicLong al = cache.cache().dataStructures().atomicLong(TEST_LONG_NAME, 0, true);

            for (int i = 0; i < operationsPerTx; i++) {
                al.addAndGet(RAND.nextInt(MAX_INT));

                long cnt = writes.incrementAndGet();

                if (cnt % WRITE_LOG_MOD == 0)
                    info("Performed " + cnt + " writes.");
            }
        }
    };

    /** Atomic long read closure. */
    private final CIX1<CacheProjection<Integer, Integer>> longReadClos =
        new CIX1<CacheProjection<Integer, Integer>>() {
        @Override public void applyx(CacheProjection<Integer, Integer> cache)
            throws IgniteCheckedException {
            CacheAtomicLong al = cache.cache().dataStructures().atomicLong(TEST_LONG_NAME, 0, true);

            for (int i = 0; i < operationsPerTx; i++) {
                al.get();

                long cnt = reads.incrementAndGet();

                if (cnt % READ_LOG_MOD == 0)
                    info("Performed " + cnt + " reads.");
            }
        }
    };

    /** Atomic reference write closure. */
    private final CIX1<CacheProjection<Integer, Integer>> refWriteClos =
        new CIX1<CacheProjection<Integer, Integer>>() {
        @Override public void applyx(CacheProjection<Integer, Integer> cache)
            throws IgniteCheckedException {
            CacheAtomicReference<Integer> ar = cache.cache().dataStructures().atomicReference(TEST_REF_NAME,
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
    private final CIX1<CacheProjection<Integer, Integer>> refReadClos =
        new CIX1<CacheProjection<Integer, Integer>>() {
        @Override public void applyx(CacheProjection<Integer, Integer> cache)
            throws IgniteCheckedException {
            CacheAtomicReference<Integer> ar = cache.cache().dataStructures().atomicReference(TEST_REF_NAME, null,
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
    private final CIX1<CacheProjection<Integer, Integer>> seqWriteClos =
        new CIX1<CacheProjection<Integer, Integer>>() {
        @Override public void applyx(CacheProjection<Integer, Integer> cache)
            throws IgniteCheckedException {
            CacheAtomicSequence as = cache.cache().dataStructures().atomicSequence(TEST_SEQ_NAME, 0, true);

            for (int i = 0; i < operationsPerTx; i++) {
                as.addAndGet(RAND.nextInt(MAX_INT) + 1);

                long cnt = writes.incrementAndGet();

                if (cnt % WRITE_LOG_MOD == 0)
                    info("Performed " + cnt + " writes.");
            }
        }
    };

    /** Atomic sequence read closure. */
    private final CIX1<CacheProjection<Integer, Integer>> seqReadClos =
        new CIX1<CacheProjection<Integer, Integer>>() {
        @Override public void applyx(CacheProjection<Integer, Integer> cache)
            throws IgniteCheckedException {
            CacheAtomicSequence as = cache.cache().dataStructures().atomicSequence(TEST_SEQ_NAME, 0, true);

            for (int i = 0; i < operationsPerTx; i++) {
                as.get();

                long cnt = reads.incrementAndGet();

                if (cnt % READ_LOG_MOD == 0)
                    info("Performed " + cnt + " reads.");
            }
        }
    };

    /** Atomic stamped write closure. */
    private final CIX1<CacheProjection<Integer, Integer>> stampWriteClos =
        new CIX1<CacheProjection<Integer, Integer>>() {
        @Override public void applyx(CacheProjection<Integer, Integer> cache)
            throws IgniteCheckedException {
            CacheAtomicStamped<Integer, Integer> as = cache.cache().dataStructures().atomicStamped(TEST_STAMP_NAME,
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
    private final CIX1<CacheProjection<Integer, Integer>> stampReadClos =
        new CIX1<CacheProjection<Integer, Integer>>() {
        @Override public void applyx(CacheProjection<Integer, Integer> cache)
            throws IgniteCheckedException {
            CacheAtomicStamped<Integer, Integer> as = cache.cache().dataStructures().atomicStamped(TEST_STAMP_NAME,
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
    private final CIX1<CacheProjection<Integer, Integer>> queueWriteClos =
        new CIX1<CacheProjection<Integer, Integer>>() {
        @Override public void applyx(CacheProjection<Integer, Integer> cache)
            throws IgniteCheckedException {
            CacheQueue<Integer> q = cache.cache().dataStructures().queue(TEST_QUEUE_NAME, 0, true, true);

            for (int i = 0; i < operationsPerTx; i++) {
                q.put(RAND.nextInt(MAX_INT));

                long cnt = writes.incrementAndGet();

                if (cnt % WRITE_LOG_MOD == 0)
                    info("Performed " + cnt + " writes.");
            }
        }
    };

    /** Queue read closure. */
    private final CIX1<CacheProjection<Integer, Integer>> queueReadClos =
        new CIX1<CacheProjection<Integer, Integer>>() {
        @Override public void applyx(CacheProjection<Integer, Integer> cache)
            throws IgniteCheckedException {
            CacheQueue<Integer> q = cache.cache().dataStructures().queue(TEST_QUEUE_NAME, 0, true, true);

            for (int i = 0; i < operationsPerTx; i++) {
                q.peek();

                long cnt = reads.incrementAndGet();

                if (cnt % READ_LOG_MOD == 0)
                    info("Performed " + cnt + " reads.");
            }
        }
    };

    /** Count down latch write closure. */
    private final CIX1<CacheProjection<Integer, Integer>> latchWriteClos =
        new CIX1<CacheProjection<Integer, Integer>>() {
        @Override public void applyx(CacheProjection<Integer, Integer> cache)
            throws IgniteCheckedException {
            CacheCountDownLatch l = cache.cache().dataStructures().countDownLatch(TEST_LATCH_NAME, LATCH_INIT_CNT,
                true, true);

            for (int i = 0; i < operationsPerTx; i++) {
                l.countDown();

                long cnt = writes.incrementAndGet();

                if (cnt % WRITE_LOG_MOD == 0)
                    info("Performed " + cnt + " writes.");
            }
        }
    };

    /** Count down latch read closure. */
    private final CIX1<CacheProjection<Integer, Integer>> latchReadClos =
        new CIX1<CacheProjection<Integer, Integer>>() {
        @Override public void applyx(CacheProjection<Integer, Integer> cache)
            throws IgniteCheckedException {
            CacheCountDownLatch l = cache.cache().dataStructures().countDownLatch(TEST_LATCH_NAME, LATCH_INIT_CNT,
                true, true);

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

                test.loadTest(test.longWriteClos, test.longReadClos);
            }

            System.gc();

            if (REF) {
                info("Testing atomic reference...");

                test.loadTest(test.refWriteClos, test.refReadClos);
            }

            System.gc();

            if (SEQ) {
                info("Testing atomic sequence...");

                test.loadTest(test.seqWriteClos, test.seqReadClos);
            }

            System.gc();

            if (STAMP) {
                info("Testing atomic stamped...");

                test.loadTest(test.stampWriteClos, test.stampReadClos);
            }

            System.gc();

            if (QUEUE) {
                info("Testing queue...");

                test.loadTest(test.queueWriteClos, test.queueReadClos);
            }

            System.gc();

            if (LATCH) {
                info("Testing count down latch...");

                test.loadTest(test.latchWriteClos, test.latchReadClos);
            }
        }
    }
}
