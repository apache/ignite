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

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.internal.util.typedef.CIX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_SWAPPED;

/**
 * Cache+swap load test.
 */
public class GridCacheSwapLoadTest {
    /** */
    private static final int LOG_MOD = 10000;

    /** */
    private static final int DFLT_KEY_CNT = 100000;

    /** */
    private static final float DFLT_GET_REMOVE_RATIO = 0.2f;

    /** */
    private static final int DFLT_PUT_THREAD_CNT = 5;

    /** */
    private static final int DFLT_GET_THREAD_CNT = 2;

    /** */
    private static final int DFLT_REMOVE_THREAD_CNT = 2;

    /** */
    private static final boolean DFLT_GET_REMOVE_ENABLED = true;

    /** */
    private static int keyCnt = DFLT_KEY_CNT;

    /** */
    private static float getRmvRatio = DFLT_GET_REMOVE_RATIO;

    /** */
    private static int putThreadCnt = DFLT_PUT_THREAD_CNT;

    /** */
    private static int getThreadCnt = DFLT_GET_THREAD_CNT;

    /** */
    private static int rmvThreadCnt = DFLT_REMOVE_THREAD_CNT;

    /** */
    private static boolean getRmvEnabled = DFLT_GET_REMOVE_ENABLED;

    /** */
    private static final CountDownLatch getRemoveStartedLatch = new CountDownLatch(1);

    /** */
    private static final BlockingQueue<Integer> swappedKeys = new LinkedBlockingQueue<>();

    /** */
    private GridCacheSwapLoadTest() {
        // No-op
    }

    /**
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        parseArgs(args);

        try (Ignite g = G.start("modules/core/src/test/config/spring-cache-swap.xml")) {
            g.events().localListen(new IgnitePredicate<Event>() {
                private final AtomicInteger cnt = new AtomicInteger(0);

                private final AtomicBoolean getRmvStartedGuard = new AtomicBoolean(false);

                @Override public boolean apply(Event evt) {
                    int cnt = this.cnt.incrementAndGet();

                    if (cnt % LOG_MOD == 0)
                        X.println(">>> Swap count: " + cnt);

                    if (getRmvEnabled) {
                        CacheEvent ce = (CacheEvent) evt;

                        Integer key = ce.key();

                        swappedKeys.add(key);

                        if (swappedKeys.size() > keyCnt * getRmvRatio &&
                            getRmvStartedGuard.compareAndSet(false, true)) {
                            getRemoveStartedLatch.countDown();

                            X.println(">>> Started get/remove.");
                        }
                    }

                    return true;
                }
            }, EVT_CACHE_OBJECT_SWAPPED);

            Collection<IgniteInternalFuture<?>> futs = new ArrayList<>(3);

            long start = System.currentTimeMillis();

            futs.add(doPut(g));

            if (getRmvEnabled)
                futs.addAll(doGetRemove(g));

            wait(futs);

            X.println("Test finished in: " + (System.currentTimeMillis() - start));
        }
    }

    /**
     * @param args Command line arguments.
     */
    private static void parseArgs(String[] args) {
        try {
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];

                switch (arg) {
                    case "-k":
                        keyCnt = Integer.valueOf(args[++i]); break;
                    case "-r":
                        getRmvRatio = Float.valueOf(args[++i]); break;
                    case "-pt":
                        putThreadCnt = Integer.valueOf(args[++i]); break;
                    case "-gt":
                        getThreadCnt = Integer.valueOf(args[++i]); break;
                    case "-rt":
                        rmvThreadCnt = Integer.valueOf(args[++i]); break;
                    case "-dgr":
                        getRmvEnabled = false; break;
                    default:
                        usage();
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();

            usage();
        }

        X.println(">>>");
        X.println(">>> Key count: " + keyCnt);
        X.println(">>> Get/remove ratio: " + getRmvRatio);
        X.println(">>> Put threads count: " + putThreadCnt);
        X.println(">>> Get threads count: " + getThreadCnt);
        X.println(">>> Remove threads count: " + rmvThreadCnt);
        X.println(">>> Get/remove " + (getRmvEnabled ? "enabled" : "disabled") + ".");
        X.println(">>>");
    }

    /** */
    private static void usage() {
        X.println(">>>");
        X.println(">>> Usage: swaploadtest.sh -k <number of keys> -r <get/remove ratio> -pt <number of put threads>");
        X.println(">>>                        -gt <number of get threads> -rt <number of remove threads> -dgr");
        X.println(">>>");
        X.println(">>> -dgr disables get/remove threads.");
        X.println(">>>");
        X.println(">>> All arguments are optional.");
        X.println(">>>");

        System.exit(1);
    }

    /**
     * @return Future.
     */
    private static IgniteInternalFuture<?> doPut(final Ignite g) {
        final AtomicInteger putKey = new AtomicInteger(0);

        return GridTestUtils.runMultiThreadedAsync(new CAX() {
            @Override public void applyx() {
                IgniteCache<Integer, Integer> cache = g.cache(null);

                assert cache != null;

                while (true) {
                    int i = putKey.incrementAndGet();

                    if (i % LOG_MOD == 0)
                        X.println(">>> Put count: " + i);

                    if (i > keyCnt)
                        break;

                    cache.put(i, i);
                }

                X.println(">>> Thread '" + Thread.currentThread().getName() + "' stopped.");
            }
        }, putThreadCnt, "put-thread");
    }

    /**
     * @return Futures.
     */
    private static Collection<IgniteInternalFuture<Long>> doGetRemove(final Ignite g) {
        final AtomicBoolean stop = new AtomicBoolean(false);

        return F.asList(
            GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    getRemoveStartedLatch.await();

                    IgniteCache<Integer, Integer> cache = g.cache(null);

                    assert cache != null;

                    while (true) {
                        Integer i = swappedKeys.take();

                        if (i == null)
                            continue;

                        Integer val = cache.get(i);

                        assert val != null && val.equals(i);

                        if (i % LOG_MOD == 0)
                            X.println(">>> Get/remove count: " + i);

                        if (i == keyCnt || stop.get()) {
                            stop.set(true);

                            break;
                        }
                    }

                    X.println(">>> Thread '" + Thread.currentThread().getName() + "' stopped.");

                    return null;
                }
            }, getThreadCnt, "get-thread"),

            GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    getRemoveStartedLatch.await();

                    IgniteCache<Integer, Integer> cache = g.cache(null);

                    assert cache != null;

                    while (true) {
                        Integer i = swappedKeys.take();

                        Integer val = cache.getAndRemove(i);

                        assert val != null && val.equals(i);

                        if (i % LOG_MOD == 0)
                            X.println(">>> Get/remove count: " + i);

                        if (i == keyCnt || stop.get()) {
                            stop.set(true);

                            break;
                        }
                    }

                    X.println(">>> Thread '" + Thread.currentThread().getName() + "' stopped.");

                    return null;
                }
            }, rmvThreadCnt, "remove-thread")
        );
    }

    /**
     * @param futs Futures.
     */
    private static void wait(Iterable<IgniteInternalFuture<?>> futs) {
        F.forEach(futs, new CIX1<IgniteInternalFuture<?>>() {
            @Override public void applyx(IgniteInternalFuture<?> fut) throws IgniteCheckedException {
                fut.get();
            }
        });
    }
}