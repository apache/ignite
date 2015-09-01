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

package org.apache.ignite.loadtests.dsi;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

/**
 *
 */
public class GridDsiPerfJob extends ComputeJobAdapter {
    /** */
    private static final ConcurrentMap<Thread, ConcurrentMap<String, T3<Long, Long, Long>>> timers =
        new ConcurrentHashMap8<>();

    /** */
    private static final long PRINT_FREQ = 10000;

    /** */
    private static final GridAtomicLong lastPrint = new GridAtomicLong();

    /** */
    private static final long MAX = 5000;

    /** */
    @IgniteInstanceResource
    private Ignite ignite;

    /** */
    private String cacheName = "PARTITIONED_CACHE";

    /**
     * @param msg Message.
     */
    public GridDsiPerfJob(@Nullable GridDsiMessage msg) {
        super(msg);
    }

    /**
     * @return Message.
     */
    @Nullable private GridDsiMessage message() {
        return argument(0);
    }

    /**
     * @return Terminal ID.
     */
    @AffinityKeyMapped
    @Nullable public String terminalId() {
        GridDsiMessage msg = message();

        return msg != null ? msg.getTerminalId() : null;
    }

    /**
     * @return Result.
     */
    @SuppressWarnings("ConstantConditions")
    @Override public Object execute() {
        ConcurrentMap<String, T2<AtomicLong, AtomicLong>> nodeLoc = ignite.cluster().nodeLocalMap();

        T2<AtomicLong, AtomicLong> cntrs = nodeLoc.get("cntrs");

        if (cntrs == null) {
            T2<AtomicLong, AtomicLong> other = nodeLoc.putIfAbsent("cntrs",
                cntrs = new T2<>(new AtomicLong(), new AtomicLong(System.currentTimeMillis())));

            if (other != null)
                cntrs = other;
        }

        long cnt = cntrs.get1().incrementAndGet();

        GridNearCacheAdapter near = (GridNearCacheAdapter)((IgniteKernal) ignite).internalCache(cacheName);
        GridDhtCacheAdapter dht = near.dht();

        doWork();

        long start = cntrs.get2().get();

        long now = System.currentTimeMillis();

        long dur = now - start;

        if (dur > 20000 && cntrs.get2().compareAndSet(start, System.currentTimeMillis())) {
            cntrs.get1().set(0);

            long txPerSec = cnt / (dur / 1000);

            X.println("Stats [tx/sec=" + txPerSec + ", nearSize=" + near.size() + ", dhtSize=" + dht.size() + ']');

            return new T3<>(txPerSec, near.size(), dht.size());
        }

        return null;
    }

    /**
     * @param name Timer name to start.
     */
    private void startTimer(String name) {
        ConcurrentMap<String, T3<Long, Long, Long>> m = timers.get(Thread.currentThread());

        if (m == null) {
            ConcurrentMap<String, T3<Long, Long, Long>> old = timers.putIfAbsent(Thread.currentThread(),
                m = new ConcurrentHashMap8<>());

            if (old != null)
                m = old;
        }

        T3<Long, Long, Long> t = m.get(name);

        if (t == null) {
            T3<Long, Long, Long> old = m.putIfAbsent(name, t = new T3<>());

            if (old != null)
                t = old;
        }

        t.set1(System.currentTimeMillis());
        t.set2(0L);
    }

    /**
     * @param name Timer name to stop.
     */
    @SuppressWarnings("ConstantConditions")
    private void stopTimer(String name) {
        ConcurrentMap<String, T3<Long, Long, Long>> m = timers.get(Thread.currentThread());

        T3<Long, Long, Long> t = m.get(name);

        assert t != null;

        long now = System.currentTimeMillis();

        t.set2(now);

        t.set3(Math.max(t.get3() == null ? 0 : t.get3(), now - t.get1()));
    }

    /**
     *
     */
    private void printTimers() {
        long now = System.currentTimeMillis();

        if (lastPrint.get() + PRINT_FREQ < now && lastPrint.setIfGreater(now)) {
            Map<String, Long> maxes = new HashMap<>();

            for (Map.Entry<Thread, ConcurrentMap<String, T3<Long, Long, Long>>> e1 : timers.entrySet()) {
                for (Map.Entry<String, T3<Long, Long, Long>> e2 : e1.getValue().entrySet()) {
                    T3<Long, Long, Long> t = e2.getValue();

                    Long start = t.get1();
                    Long end = t.get2();

                    assert start != null;
                    assert end != null;

                    long duration = end == 0 ? now - start : end - start;

                    long max = t.get3() == null ? duration : t.get3();

                    if (duration < 0)
                        duration = now - start;

                    if (duration > MAX)
                        X.println("Maxed out timer [name=" + e2.getKey() + ", duration=" + duration +
                            ", ongoing=" + (end == 0) + ", thread=" + e1.getKey().getName() + ']');

                    Long cmax = maxes.get(e2.getKey());

                    if (cmax == null || max > cmax)
                        maxes.put(e2.getKey(), max);

                    t.set3(null);
                }
            }

            for (Map.Entry<String, Long> e : maxes.entrySet())
                X.println("Timer [name=" + e.getKey() + ", maxTime=" + e.getValue() + ']');

            X.println(">>>>");
        }
    }

    /**
     *
     */
    private void doWork() {
        IgniteCache cache = ignite.cache(cacheName);

        assert cache != null;

        // This is instead of former code to find request
        // with some ID.
        try {
            getId();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        startTimer("getSession");

        String terminalId = terminalId();

        assert terminalId != null;

        GridDsiSession ses = null;

        try {
            ses = (GridDsiSession)get(GridDsiSession.getCacheKey(terminalId));
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        stopTimer("getSession");

        if (ses == null)
            ses = new GridDsiSession(terminalId);

        try {
            try (Transaction tx = ignite.transactions().txStart()) {
                GridDsiRequest req = new GridDsiRequest(getId());

                req.setMessageId(getId());

                startTimer("putRequest");

                put(req, req.getCacheKey(terminalId));

                stopTimer("putRequest");

                for (int i = 0; i < 5; i++) {
                    GridDsiResponse rsp = new GridDsiResponse(getId());

                    startTimer("putResponse-" + i);

                    put(rsp, rsp.getCacheKey(terminalId));

                    stopTimer("putResponse-" + i);
                }

                startTimer("putSession");

                put(ses, ses.getCacheKey());

                stopTimer("putSession");

                startTimer("commit");

                tx.commit();

                stopTimer("commit");
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        printTimers();
    }

    /**
     * @return ID.
     */
    private long getId() {
        IgniteAtomicSequence seq = ignite.atomicSequence("ID", 0, true);

        return seq.incrementAndGet();
    }

    /**
     * @param o Object.
     * @param cacheKey Key.
     */
    private void put(final Object o, Object cacheKey) {
        IgniteCache<Object, Object> cache = ignite.cache(cacheName);

        assert cache != null;

        cache.invoke(cacheKey, new EntryProcessor<Object, Object, Cache.Entry<Object, Object>>() {
            @Override public Cache.Entry<Object, Object> process(MutableEntry<Object, Object> entry, Object... arguments)
                throws EntryProcessorException {
                if (entry != null)
                    entry.setValue(o);

                return null;
            }
        });
    }

    /**
     * @param key Key.
     * @return Object.
     */
    @SuppressWarnings("ConstantConditions")
    private <T> Object get(Object key) {
        return ignite.cache(cacheName).get(key);
    }
}