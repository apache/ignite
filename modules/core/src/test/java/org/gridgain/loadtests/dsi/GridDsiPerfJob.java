/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.dsi;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

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
    @GridCacheName
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
    @GridCacheAffinityKeyMapped
    @Nullable public String terminalId() {
        GridDsiMessage msg = message();

        return msg != null ? msg.getTerminalId() : null;
    }

    /**
     * @return Result.
     */
    @SuppressWarnings("ConstantConditions")
    @Override public Object execute() {
        ClusterNodeLocalMap<String, T2<AtomicLong, AtomicLong>> nodeLoc = ignite.cluster().nodeLocalMap();

        T2<AtomicLong, AtomicLong> cntrs = nodeLoc.get("cntrs");

        if (cntrs == null) {
            T2<AtomicLong, AtomicLong> other = nodeLoc.putIfAbsent("cntrs",
                cntrs = new T2<>(new AtomicLong(), new AtomicLong(System.currentTimeMillis())));

            if (other != null)
                cntrs = other;
        }

        long cnt = cntrs.get1().incrementAndGet();

        GridNearCacheAdapter near = (GridNearCacheAdapter)((GridKernal) ignite).internalCache(cacheName);
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
        GridCache cache = ignite.cache(cacheName);

        assert cache != null;

        // This is instead of former code to find request
        // with some ID.
        try {
            getId();
        }
        catch (IgniteCheckedException e) {
            e.printStackTrace();
        }

        startTimer("getSession");

        String terminalId = terminalId();

        assert terminalId != null;

        GridDsiSession ses = null;

        try {
            ses = (GridDsiSession)get(GridDsiSession.getCacheKey(terminalId));
        }
        catch (IgniteCheckedException e) {
            e.printStackTrace();
        }

        stopTimer("getSession");

        if (ses == null)
            ses = new GridDsiSession(terminalId);

        try {
            try (GridCacheTx tx = cache.txStart()) {
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
     * @throws IgniteCheckedException If failed.
     */
    private long getId() throws IgniteCheckedException {
        GridCache<Object, Object> cache = ignite.cache(cacheName);

        assert cache != null;

        GridCacheAtomicSequence seq = cache.dataStructures().atomicSequence("ID", 0, true);

        return seq.incrementAndGet();
    }

    /**
     * @param o Object.
     * @param cacheKey Key.
     * @throws IgniteCheckedException If failed.
     */
    private void put(Object o, Object cacheKey) throws IgniteCheckedException {
        GridCache<Object, Object> cache = ignite.cache(cacheName);

        assert cache != null;

        GridCacheEntry<Object, Object> entry = cache.entry(cacheKey);

        if (entry != null)
            entry.setx(o);
    }

    /**
     * @param key Key.
     * @return Object.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("ConstantConditions")
    private <T> Object get(Object key) throws IgniteCheckedException {
        return ignite.cache(cacheName).get(key);
    }
}
