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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cache.query.annotations.QueryGroupIndex;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheEntry;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T5;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.NONE;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class GridCacheConcurrentTxMultiNodeTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Timers. */
    private static final ConcurrentMap<Thread, ConcurrentMap<String, T5<Long, Long, Long, IgniteUuid, Object>>> timers =
        new ConcurrentHashMap<>();

    /** */
    private static final long PRINT_FREQ = 10000;

    /** */
    private static final GridAtomicLong lastPrint = new GridAtomicLong();

    /** */
    private static final IgnitePredicate<ClusterNode> serverNode = new P1<ClusterNode>() {
        @Override public boolean apply(ClusterNode n) {
            String gridName = G.ignite(n.id()).name();

            return gridName != null && gridName.contains("server");
        }
    };

    /** */
    private static final IgnitePredicate<ClusterNode> clientNode = new P1<ClusterNode>() {
        @Override public boolean apply(ClusterNode n) {
            String gridName = G.ignite(n.id()).name();

            return gridName != null && gridName.contains("client");
        }
    };

    /** */
    private CacheMode mode = PARTITIONED;

    /** */
    private boolean cacheOn;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.getTransactionConfiguration().setDefaultTxConcurrency(PESSIMISTIC);
        c.getTransactionConfiguration().setDefaultTxIsolation(REPEATABLE_READ);

        AtomicConfiguration atomicCfg = new AtomicConfiguration();

        atomicCfg.setAtomicSequenceReserveSize(100000);
        atomicCfg.setCacheMode(mode);

        c.setAtomicConfiguration(atomicCfg);

        if (cacheOn) {
            CacheConfiguration cc = defaultCacheConfiguration();

            cc.setCacheMode(mode);

            LruEvictionPolicy plc = new LruEvictionPolicy();
            plc.setMaxSize(1000);

            cc.setEvictionPolicy(plc);
            cc.setEvictSynchronized(false);
            cc.setSwapEnabled(false);
            cc.setWriteSynchronizationMode(FULL_SYNC);
            cc.setRebalanceMode(NONE);

            c.setCacheConfiguration(cc);
        }
        else
            c.setCacheConfiguration();

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        c.setPeerClassLoadingEnabled(false);

        // Enable tracing.
//        Logger.getLogger("org.apache.ignite.kernal.processors.cache.GridCacheDgcManager.trace").setLevel(Level.DEBUG);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE;
    }

    /**
     * @throws Exception If failed.
     */
    public void testEvictions() throws Exception {
        try {
            cacheOn = true;

            Ignite srvr1 = startGrid("server1");

            srvr1.atomicSequence("ID", 0, true);

            startGrid("server2");

            cacheOn = false;

            // Client processes count.
            int clientCnt = 8;

            for (int i = 1; i <= clientCnt; i++)
                startGrid("client" + i);

            Collection<ClusterNode> srvrNodes = srvr1.cluster().forPredicate(serverNode).nodes();
            Collection<ClusterNode> clientNodes = srvr1.cluster().forPredicate(clientNode).nodes();

            assert srvrNodes.size() == 2;

            // Threads count per each client process.
            int threadCnt = 2;

            int srvrMaxNoTerminals = threadCnt / srvrNodes.size();

            if (srvrMaxNoTerminals *  srvrNodes.size() != threadCnt) {
                threadCnt = srvrMaxNoTerminals * srvrNodes.size();

                info("Using " + threadCnt + " threads instead to ensure equal distribution of terminals");
            }

            Collection<Callable<Object>> clients = new ArrayList<>(threadCnt * clientCnt);

            info("No of servers: " + srvrNodes.size());
            info("No of clients: " + clientNodes.size());
            info("Thread count: " + threadCnt);
            info("Max number of terminals / server: " + srvrMaxNoTerminals);

            // Distribute terminals evenly across all servers
            for (ClusterNode node : srvrNodes) {
                UUID srvrId = node.id();

                info(">>> Node ID: " + srvrId);

                int terminalsPerSrvr = 0;

                int tid = 0; // Terminal ID.

                while (true) {
                    String terminalId = String.valueOf(++tid);

                    // Server partition cache
                    UUID mappedId = srvr1.cluster().mapKeyToNode(null, terminalId).id();

                    if (!srvrId.equals(mappedId))
                        continue;

                    info("Affinity mapping [key=" + terminalId + ", nodeId=" + mappedId + ']');

                    for (int i = 1; i <= clientCnt; i++)
                        clients.add(new Client(G.ignite("client" + i), terminalId, srvrId));

                    info("Terminal ID: " + terminalId);

                    terminalsPerSrvr++;

                    if (terminalsPerSrvr == srvrMaxNoTerminals)
                        break;
                }
            }

            displayReqCount();

            ExecutorService pool = Executors.newFixedThreadPool(clients.size());

            pool.invokeAll(clients);

            Thread.sleep(Long.MAX_VALUE);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private void displayReqCount() {
        new Thread(new Runnable() {
            @SuppressWarnings({"BusyWait", "InfiniteLoopStatement"})
            @Override public void run() {
                int interval = 10;

                while (true) {
                    long cnt0 = Client.txCnt.get();
                    long lt0 = Client.latency.get();

                    try {
                        Thread.sleep(interval * 1000);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    long cnt1 = Client.txCnt.get();
                    long lt1 = Client.latency.get();

                    info(">>>");
                    info(">>> Transaction/s: " + (cnt1 - cnt0) / interval);
                    info(">>> Avg Latency: " + ((cnt1 - cnt0) > 0 ? (lt1 - lt0) / (cnt1 - cnt0) + "ms" : "invalid"));
                    info(">>> Max Submit Time: " + Client.submitTime.getAndSet(0));

                    try {
                        PerfJob.printTimers();
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    /**
     *
     */
    private static class Client implements Callable<Object> {
        /** */
        private static AtomicLong txCnt = new AtomicLong();

        /** */
        private static AtomicLong latency = new AtomicLong();

        /** */
        private static GridAtomicLong submitTime = new GridAtomicLong();


        /** */
        private Ignite g;

        /** */
        private String terminalId;

        /** */
        private UUID nodeId;

        /**
         * @param g Grid.
         * @param terminalId Terminal ID.
         * @param nodeId Node ID.
         */
        private Client(Ignite g, String terminalId, UUID nodeId) {
            this.g = g;
            this.terminalId = terminalId;
            this.nodeId = nodeId;
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"InfiniteLoopStatement"})
        @Override public Object call() throws Exception {
            while (true) {
                try {
                    long t0 = System.currentTimeMillis();

                    long submitTime1 = t0;

                    IgniteCompute comp = g.compute(g.cluster().forPredicate(serverNode)).withAsync();

                    comp.execute(RequestTask.class, new Message(terminalId, nodeId));

                    ComputeTaskFuture<Void> f1 = comp.future();

                    submitTime.setIfGreater(System.currentTimeMillis() - submitTime1);

                    f1.get();

                    submitTime1 = System.currentTimeMillis();

                    comp.execute(ResponseTask.class, new Message(terminalId, nodeId));

                    ComputeTaskFuture<Void> f2 = comp.future();

                    submitTime.setIfGreater(System.currentTimeMillis() - submitTime1);

                    f2.get();

                    long t1 = System.currentTimeMillis();

                    txCnt.incrementAndGet();

                    latency.addAndGet(t1 - t0);
                }
                catch (IgniteException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     *
     */
    private static class Message implements Serializable {
        /** */
        private String terminalId;

        /** */
        private UUID nodeId;

        /**
         * @param terminalId Terminal ID.
         * @param nodeId Node ID.
         */
        Message(String terminalId, UUID nodeId) {
            this.terminalId = terminalId;
            this.nodeId = nodeId;
        }

        /**
         * @return Terminal ID.
         */
        String getTerminalId() {
            return terminalId;
        }

        /**
         * @param terminalId Terminal ID.
         */
        void setTerminalId(String terminalId) {
            this.terminalId = terminalId;
        }

        /**
         * @return Node ID.
         */
        UUID getNodeId() {
            return nodeId;
        }

        /**
         * @param nodeId Node ID.
         */
        void setNodeId(UUID nodeId) {
            this.nodeId = nodeId;
        }
    }

    /**
     *
     */
    private static class PerfJob extends ComputeJobAdapter {
        /** */
        private static final long MAX = 5000;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         * @param msg Message.
         */
        PerfJob(@Nullable Message msg) {
            super(msg);
        }

        /**
         * @return Message.
         */
        private Message message() {
            return argument(0);
        }

        /**
         * @return Terminal ID.
         */
        @AffinityKeyMapped
        public String terminalId() {
            return message().getTerminalId();
        }

        /** {@inheritDoc} */
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

            doWork();

            GridNearCacheAdapter near = (GridNearCacheAdapter)((IgniteKernal) ignite).internalCache();
            GridDhtCacheAdapter dht = near.dht();

            long start = cntrs.get2().get();

            long now = System.currentTimeMillis();

            long dur = now - start;

            if (dur > 20000 && cntrs.get2().compareAndSet(start, System.currentTimeMillis())) {
                cntrs.get1().set(0);

                X.println("Stats [tx/sec=" + (cnt / (dur / 1000)) + ", nearSize=" + near.size() +
                    ", dhtSize=" + dht.size() + ']');
            }

            return null;
        }

        /**
         * @param name Timer name.
         * @param xid XID.
         * @param key Key.
         * @param termId Terminal ID.
         */
        private void startTimer(String name, @Nullable IgniteUuid xid, @Nullable String key, String termId) {
            ConcurrentMap<String, T5<Long, Long, Long, IgniteUuid, Object>> m = timers.get(Thread.currentThread());

            if (m == null) {
                ConcurrentMap<String, T5<Long, Long, Long, IgniteUuid, Object>> old =
                    timers.putIfAbsent(Thread.currentThread(),
                        m = new ConcurrentHashMap<>());

                if (old != null)
                    m = old;
            }

            T5<Long, Long, Long, IgniteUuid, Object> t = m.get(name);

            if (t == null) {
                T5<Long, Long, Long, IgniteUuid, Object> old = m.putIfAbsent(name,
                    t = new T5<>());

                if (old != null)
                    t = old;
            }

            t.set1(System.currentTimeMillis());
            t.set2(0L);
            t.set4(xid);
            t.set5(key == null ? null : new AffinityKey<String>(key, termId) {});
        }

        /**
         * @param name Timer name.
         */
        private void stopTimer(String name) {
            ConcurrentMap<String, T5<Long, Long, Long, IgniteUuid, Object>> m = timers.get(Thread.currentThread());

            T5<Long, Long, Long, IgniteUuid, Object> t = m.get(name);

            assert t != null;

            long now = System.currentTimeMillis();

            t.set2(now);
            t.set3(Math.max(t.get3() == null ? 0 : t.get3(), now - t.get1()));
            t.set4(null);
            t.set5(null);
        }

        /**
         * @throws Exception If failed.
         */
        private static void printTimers() throws Exception {
            //String termId = terminalId();

            long now = System.currentTimeMillis();

            if (lastPrint.get() + PRINT_FREQ < now && lastPrint.setIfGreater(now)) {
                Map<String, Long> maxes = new HashMap<>();

                Set<AffinityKey<String>> keys = null;

                for (Map.Entry<Thread, ConcurrentMap<String, T5<Long, Long, Long, IgniteUuid, Object>>> e1 : timers.entrySet()) {
                    for (Map.Entry<String, T5<Long, Long, Long, IgniteUuid, Object>> e2 : e1.getValue().entrySet()) {
                        T5<Long, Long, Long, IgniteUuid, Object> t = e2.getValue();

                        long start = t.get1();
                        long end = t.get2();

                        IgniteUuid xid = t.get4();

                        long duration = end == 0 ? now - start : end - start;

                        long max = t.get3() == null ? duration : t.get3();

                        if (duration < 0)
                            duration = now - start;

                        if (duration > MAX) {
                            X.println("Maxed out timer [name=" + e2.getKey() + ", key=" + t.get5() +
                                ", duration=" + duration + ", ongoing=" + (end == 0) +
                                ", thread=" + e1.getKey().getName() + ", xid=" + xid + ']');

                            AffinityKey<String> key = (AffinityKey<String>)t.get5();

                            if (key != null) {
                                if (keys == null)
                                    keys = new LinkedHashSet<>();

                                keys.add(key);
                            }
                        }

                        Long cmax = maxes.get(e2.getKey());

                        if (cmax == null || max > cmax)
                            maxes.put(e2.getKey(), max);

                        t.set3(null);
                    }
                }

                if (!F.isEmpty(keys)) {
                    for (Ignite g : G.allGrids()) {
                        if (g.name().contains("server")) {
                            GridNearCacheAdapter<AffinityKey<String>, Object> near =
                                (GridNearCacheAdapter<AffinityKey<String>, Object>)((IgniteKernal)g).
                                    <AffinityKey<String>, Object>internalCache();
                            GridDhtCacheAdapter<AffinityKey<String>, Object> dht = near.dht();

                            for (AffinityKey<String> k : keys) {
                                GridNearCacheEntry nearEntry = (GridNearCacheEntry)near.peekEx(k);
                                GridDhtCacheEntry dhtEntry = (GridDhtCacheEntry)dht.peekEx(k);

                                X.println("Near entry [grid="+ g.name() + ", key=" + k + ", entry=" + nearEntry);
                                X.println("DHT entry [grid=" + g.name() + ", key=" + k + ", entry=" + dhtEntry);

                                GridCacheMvccCandidate nearCand =
                                    nearEntry == null ? null : F.first(nearEntry.localCandidates());

                                if (nearCand != null)
                                    X.println("Near futures: " +
                                        nearEntry.context().mvcc().futures(nearCand.version()));

                                GridCacheMvccCandidate dhtCand =
                                    dhtEntry == null ? null : F.first(dhtEntry.localCandidates());

                                if (dhtCand != null)
                                    X.println("Dht futures: " +
                                        dhtEntry.context().mvcc().futures(dhtCand.version()));

                            }
                        }
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
        private void doWork()  {
            Session ses = new Session(terminalId());

            try {
                try (Transaction tx = ignite.transactions().txStart()) {
                    Request req = new Request(getId());

                    req.setMessageId(getId());

                    String key = req.getCacheKey();

                    startTimer("putRequest", tx.xid(), key, terminalId());

                    put(req, key, terminalId());

                    stopTimer("putRequest");
//
//                    for (int i = 0; i < 5; i++) {
//                        Response rsp = new Response(getId());
//
//                        startTimer("putResponse-" + i, tx.xid());
//
//                        put(rsp, rsp.getCacheKey(), terminalId());
//
//                        stopTimer("putResponse-" + i);
//                    }

                    key = ses.getCacheKey();

                    startTimer("putSession", tx.xid(), key, terminalId());

                    put(ses, key, terminalId());

                    stopTimer("putSession");

                    startTimer("commit", tx.xid(), null, terminalId());

                    tx.commit();

                    stopTimer("commit");
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        /**
         * @return New ID.
         */
        private long getId() {
            IgniteAtomicSequence seq = ignite.atomicSequence("ID", 0, true);

            return seq.incrementAndGet();
        }

        /**
         * @param o Object to put.
         * @param cacheKey Cache key.
         * @param terminalId Terminal ID.
         */
        private void put(Object o, String cacheKey, String terminalId) {
//            CacheProjection<AffinityKey<String>, Object> cache = ((IgniteKernal)ignite).cache(null);
//
//            AffinityKey<String> affinityKey = new AffinityKey<>(cacheKey, terminalId);
//
//            Entry<AffinityKey<String>, Object> entry = cache.entry(affinityKey);
//
//            entry.setx(o);
            assert false;
        }

        /**
         * @param cacheKey Cache key.
         * @param terminalId Terminal ID.
         * @return Cached object.
         */
        @SuppressWarnings({"RedundantCast"})
        private <T> Object get(String cacheKey, String terminalId) {
            Object key = new AffinityKey<>(cacheKey, terminalId);

            return (T) ignite.cache(null).get(key);
        }
    }

    /**
     *
     */
    @QueryGroupIndex(name = "msg_tx")
    @SuppressWarnings({"UnusedDeclaration"})
    private static class Request implements Serializable {
        /** */
        @QuerySqlField(index = true)
        private Long id;

        /** */
        @QuerySqlField(name = "messageId")
        @QuerySqlField.Group(name = "msg_tx", order = 3)
        private long msgId;

        /** */
        @QuerySqlField(name = "transactionId")
        @QuerySqlField.Group(name = "msg_tx", order = 1)
        private long txId;

        /**
         * @param id Request ID.
         */
        Request(long id) {
            this.id = id;
        }

        /**
         * @param msgId Message ID.
         */
        public void setMessageId(long msgId) {
            this.msgId = msgId;
        }

        /**
         * @return Cache key.
         */
        public String getCacheKey() {
            return "RESPONSE:" + id.toString();
        }
    }

    /**
     *
     */
    @SuppressWarnings({"UnusedDeclaration"})
    private static class Response implements Serializable {
        /** */
        @QuerySqlField
        private Long id;

        /** */
        @QuerySqlField(name = "messageId")
        private long msgId;

        /** */
        @QuerySqlField(name = "transactionId")
        private long txId;

        /**
         * @param id Response ID.
         */
        Response(long id) {
            this.id = id;
        }

        /**
         * @return Cache key.
         */
        public String getCacheKey() {
            return "REQUEST:" + id.toString();
        }
    }

    /**
     *
     */
    private static class Session implements Serializable {
        /** */
        @QuerySqlField(index = true)
        private String terminalId;

        /**
         * @param terminalId Terminal ID.
         */
        Session(String terminalId) {
            this.terminalId = terminalId;
        }

        /**
         * @return Cache key.
         */
        public String getCacheKey() {
            return "SESSION:" + terminalId;
        }
    }

    /**
     *
     */
    @SuppressWarnings( {"UnusedDeclaration"})
    private static class ResponseTask extends ComputeTaskSplitAdapter<Message, Void> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int arg0, Message msg) {
            return Collections.singletonList(new PerfJob(msg));
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<ComputeJobResult> results) {
            return null;
        }
    }

    /**
     *
     */
    private static class RequestTask extends ComputeTaskSplitAdapter<Message, Void> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int arg0, Message msg) {
            return Collections.singletonList(new PerfJob(msg));
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<ComputeJobResult> results) {
            return null;
        }
    }
}