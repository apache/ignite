/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.cache.eviction.lru.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 *
 */
public class GridCacheConcurrentTxMultiNodeTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

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
            String gridName = G.grid(n.id()).name();

            return gridName != null && gridName.contains("server");
        }
    };

    /** */
    private static final IgnitePredicate<ClusterNode> clientNode = new P1<ClusterNode>() {
        @Override public boolean apply(ClusterNode n) {
            String gridName = G.grid(n.id()).name();

            return gridName != null && gridName.contains("client");
        }
    };

    /** */
    private GridCacheMode mode = PARTITIONED;

    /** */
    private boolean cacheOn;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.getTransactionsConfiguration().setDefaultTxConcurrency(PESSIMISTIC);
        c.getTransactionsConfiguration().setDefaultTxIsolation(REPEATABLE_READ);

        if (cacheOn) {
            GridCacheConfiguration cc = defaultCacheConfiguration();

            cc.setCacheMode(mode);
            cc.setDistributionMode(PARTITIONED_ONLY);
            cc.setEvictionPolicy(new GridCacheLruEvictionPolicy(1000));
            cc.setEvictSynchronized(false);
            cc.setEvictNearSynchronized(false);
            cc.setSwapEnabled(false);
            cc.setWriteSynchronizationMode(FULL_SYNC);
            cc.setAtomicSequenceReserveSize(100000);
            cc.setPreloadMode(NONE);

            c.setCacheConfiguration(cc);
        }
        else
            c.setCacheConfiguration();

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        c.setPeerClassLoadingEnabled(false);

        // Enable tracing.
//        Logger.getLogger("org.gridgain.grid.kernal.processors.cache.GridCacheDgcManager.trace").setLevel(Level.DEBUG);

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

            srvr1.cache(null).dataStructures().atomicSequence("ID", 0, true);

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
                        clients.add(new Client(G.grid("client" + i), terminalId, srvrId));

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

                    IgniteCompute comp = g.compute(g.cluster().forPredicate(serverNode)).enableAsync();

                    comp.execute(RequestTask.class, new Message(terminalId, nodeId));

                    GridComputeTaskFuture<Void> f1 = comp.future();

                    submitTime.setIfGreater(System.currentTimeMillis() - submitTime1);

                    f1.get();

                    submitTime1 = System.currentTimeMillis();

                    comp.execute(ResponseTask.class, new Message(terminalId, nodeId));

                    GridComputeTaskFuture<Void> f2 = comp.future();

                    submitTime.setIfGreater(System.currentTimeMillis() - submitTime1);

                    f2.get();

                    long t1 = System.currentTimeMillis();

                    txCnt.incrementAndGet();

                    latency.addAndGet(t1 - t0);
                }
                catch (GridException e) {
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
    private static class PerfJob extends GridComputeJobAdapter {
        /** */
        private static final long MAX = 5000;

        /** */
        @GridInstanceResource
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
        @GridCacheAffinityKeyMapped
        public String terminalId() {
            return message().getTerminalId();
        }

        /** {@inheritDoc} */
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

            doWork();

            GridNearCacheAdapter near = (GridNearCacheAdapter)((GridKernal) ignite).internalCache();
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
            t.set5(key == null ? null : new GridCacheAffinityKey<String>(key, termId) {});
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

                Set<GridCacheAffinityKey<String>> keys = null;

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

                            GridCacheAffinityKey<String> key = (GridCacheAffinityKey<String>)t.get5();

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
                            GridNearCacheAdapter<GridCacheAffinityKey<String>, Object> near =
                                (GridNearCacheAdapter<GridCacheAffinityKey<String>, Object>)((GridKernal)g).
                                    <GridCacheAffinityKey<String>, Object>internalCache();
                            GridDhtCacheAdapter<GridCacheAffinityKey<String>, Object> dht = near.dht();

                            for (GridCacheAffinityKey<String> k : keys) {
                                GridNearCacheEntry<?, ?> nearEntry = near.peekExx(k);
                                GridDhtCacheEntry<?, ?> dhtEntry = dht.peekExx(k);

                                X.println("Near entry [grid="+ g.name() + ", key=" + k + ", entry=" + nearEntry);
                                X.println("DHT entry [grid=" + g.name() + ", key=" + k + ", entry=" + dhtEntry);

                                GridCacheMvccCandidate<?> nearCand =
                                    nearEntry == null ? null : F.first(nearEntry.localCandidates());

                                if (nearCand != null)
                                    X.println("Near futures: " +
                                        nearEntry.context().mvcc().futures(nearCand.version()));

                                GridCacheMvccCandidate<?> dhtCand =
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
            GridCache cache = ignite.cache(null);

            Session ses = new Session(terminalId());

            try {
                try (GridCacheTx tx = cache.txStart()) {
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
            catch (GridException e) {
                e.printStackTrace();
            }
        }

        /**
         * @return New ID.
         * @throws GridException If failed.
         */
        private long getId() throws GridException {
            GridCacheAtomicSequence seq = ignite.cache(null).dataStructures().atomicSequence("ID", 0, true);
            return seq.incrementAndGet();
        }

        /**
         * @param msgId Message ID.
         * @return Request.
         */
        private Request findRequestWithMessageId(Long msgId) {
            GridCacheProjection<Object, Request> cache = ignite.cache(null).projection(Object.class, Request.class);

            GridCacheQuery<Map.Entry<Object, Request>> qry = cache.queries().createSqlQuery(
                Request.class, "messageId = ?");

            try {
                // taking out localNode() doesn't change the eviction timeout future
                // problem
                Map.Entry<Object, Request> entry =
                    F.first(qry.projection(ignite.cluster().forLocal()).execute(msgId).get());

                if (entry == null)
                    return null;

                return entry.getValue();
            }
            catch (GridException e) {
                e.printStackTrace();

                return null;
            }
        }

        /**
         * @param o Object to put.
         * @param cacheKey Cache key.
         * @param terminalId Terminal ID.
         * @throws GridException If failed.
         */
        private void put(Object o, String cacheKey, String terminalId) throws GridException {
            GridCache<GridCacheAffinityKey<String>, Object> cache = ignite.cache(null);

            GridCacheAffinityKey<String> affinityKey = new GridCacheAffinityKey<>(cacheKey, terminalId);

            GridCacheEntry<GridCacheAffinityKey<String>, Object> entry = cache.entry(affinityKey);

            entry.setx(o);
        }

        /**
         * @param cacheKey Cache key.
         * @param terminalId Terminal ID.
         * @return Cached object.
         * @throws GridException If failed.
         */
        @SuppressWarnings({"RedundantCast"})
        private <T> Object get(String cacheKey, String terminalId) throws GridException {
            Object key = new GridCacheAffinityKey<>(cacheKey, terminalId);

            return (T) ignite.cache(null).get(key);
        }
    }

    /**
     *
     */
    @GridCacheQueryGroupIndex(name = "msg_tx", unique = true)
    @SuppressWarnings({"UnusedDeclaration"})
    private static class Request implements Serializable {
        /** */
        @GridCacheQuerySqlField(unique = true)
        private Long id;

        /** */
        @GridCacheQuerySqlField(name = "messageId")
        @GridCacheQuerySqlField.Group(name = "msg_tx", order = 3)
        private long msgId;

        /** */
        @GridCacheQuerySqlField(name = "transactionId")
        @GridCacheQuerySqlField.Group(name = "msg_tx", order = 1)
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
        @GridCacheQuerySqlField(unique = true)
        private Long id;

        /** */
        @GridCacheQuerySqlField(name = "messageId")
        private long msgId;

        /** */
        @GridCacheQuerySqlField(name = "transactionId")
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
        @GridCacheQuerySqlField(unique = true)
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
    private static class ResponseTask extends GridComputeTaskSplitAdapter<Message, Void> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int arg0, Message msg) throws GridException {
            return Collections.singletonList(new PerfJob(msg));
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<GridComputeJobResult> results) throws GridException {
            return null;
        }
    }

    /**
     *
     */
    private static class RequestTask extends GridComputeTaskSplitAdapter<Message, Void> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int arg0, Message msg) throws GridException {
            return Collections.singletonList(new PerfJob(msg));
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<GridComputeJobResult> results) throws GridException {
            return null;
        }
    }
}
