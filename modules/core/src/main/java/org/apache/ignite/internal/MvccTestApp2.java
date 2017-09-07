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

package org.apache.ignite.internal;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jsr166.ConcurrentHashMap8;

/**
 *
 */
public class MvccTestApp2 {
    /** */
    private static final boolean DEBUG_LOG = false;

    /** */
    private static final boolean SQL = false;

    public static void main1(String[] args) throws Exception {
        final TestCluster cluster = new TestCluster(1);

        final int ACCOUNTS = 3;

        final int START_VAL = 10;

        final Map<Object, Object> data = new TreeMap<>();

        for (int i = 0; i < ACCOUNTS; i++)
            data.put(i, START_VAL);

        cluster.txPutAll(data);

        cluster.txTransfer(0, 1, true);
        cluster.txTransfer(0, 1, true);
        cluster.txTransfer(0, 2, true);

        Map<Object, Object> vals = cluster.sqlAll();

        System.out.println();

        Map<Object, Object> getData = cluster.sqlAll();;//cluster.getAll(data.keySet());

        int sum = 0;

        for (int i = 0; i < ACCOUNTS; i++) {
            Integer val = (Integer)getData.get(i);

            sum += val;

            System.out.println("Val: " + val);
        }

        System.out.println("Sum: " + sum);

        cluster.cleanup();

        getData = cluster.sqlAll();

        System.out.println();
//
//        MvccQueryVersion ver1 = cluster.crd.queryVersion();
//        MvccQueryVersion ver2 = cluster.crd.queryVersion();
//
//        cluster.crd.queryDone(ver2.cntr);
//        cluster.crd.queryDone(ver1.cntr);
    }

    public static void main0(String[] args) throws Exception {
        final TestCluster cluster = new TestCluster(1);

        final int ACCOUNTS = 3;

        final int START_VAL = 10;

        final Map<Object, Object> data = new TreeMap<>();

        for (int i = 0; i < ACCOUNTS; i++)
            data.put(i, START_VAL);

        cluster.txPutAll(data);

        //cluster.txRemoveTransfer(0, 1);

        Map<Object, Object> getData = cluster.sqlAll();;//cluster.getAll(data.keySet());

        int sum = 0;

        for (Map.Entry<Object, Object> e : getData.entrySet()) {
            Integer val = (Integer)e.getValue();

            if (val != null)
                sum += val;

            System.out.println("Val: " + val);
        }

        System.out.println("Sum: " + sum);

        cluster.cleanup();

        getData = cluster.sqlAll();

        System.out.println();
//
//        MvccQueryVersion ver1 = cluster.crd.queryVersion();
//        MvccQueryVersion ver2 = cluster.crd.queryVersion();
//
//        cluster.crd.queryDone(ver2.cntr);
//        cluster.crd.queryDone(ver1.cntr);
    }

    public static void main(String[] args) throws Exception {
        final AtomicBoolean err = new AtomicBoolean();

        final int READ_THREADS = 4;
        final int UPDATE_THREADS = 4;
        final int ACCOUNTS = 50;

        final int START_VAL = 100000;

        for (int iter = 0; iter < 1000; iter++) {
            System.out.println("Iteration [readThreads=" + READ_THREADS +
                ", updateThreads=" + UPDATE_THREADS + ", accounts=" + ACCOUNTS + ", iter=" + iter + ']');

            final TestCluster cluster = new TestCluster(1);

            final Map<Object, Object> data = new TreeMap<>();

            for (int i = 0; i < ACCOUNTS; i++)
                data.put(i, START_VAL);

            cluster.txPutAll(data);

            final AtomicBoolean stop = new AtomicBoolean();

            List<Thread> threads = new ArrayList<>();

            Thread cleanupThread = new Thread(new Runnable() {
                @Override public void run() {
                    Thread.currentThread().setName("cleanup");

                    try {
                        while (!stop.get()) {
                            cluster.cleanup();

                            Thread.sleep(1);
                        }
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });

            threads.add(cleanupThread);

            cleanupThread.start();

            final boolean REMOVES = false;

            for (int i = 0; i < READ_THREADS; i++) {
                final int id = i;

                Thread thread = new Thread(new Runnable() {
                    @Override public void run() {
                        try {
                            Thread.currentThread().setName("read" + id);

                            int cnt = 0;

                            while (!stop.get()) {
                                Map<Object, Object> qryData = SQL ? cluster.sqlAll() : cluster.getAll(data.keySet());

                                cnt++;

                                int sum = 0;

                                if (REMOVES) {
                                    for (Map.Entry<Object, Object> e : qryData.entrySet()) {
                                        Integer val = (Integer)e.getValue();

                                        if (val != null)
                                            sum += val;
                                        else
                                            System.out.println("With null");
                                    }
                                }
                                else {
                                    for (int i = 0; i < ACCOUNTS; i++) {
                                        Integer val = (Integer)qryData.get(i);

                                        if (val == null) {
                                            if (stop.compareAndSet(false, true)) {
                                                err.set(true);
                                                stop.set(true);

                                                TestDebugLog.printAllAndExit("No value for key: " + i);
                                            }

                                            return;
                                        }

                                        sum += val;
                                    }
                                }

                                if (sum != ACCOUNTS * START_VAL) {
                                    if (stop.compareAndSet(false, true)) {
                                        err.set(true);
                                        stop.set(true);

                                        TestDebugLog.printAllAndExit("Invalid get sum: " + sum);
                                    }
                                }
                            }

                            System.out.println("Get cnt: " + cnt);
                        }
                        catch (Throwable e) {
                            e.printStackTrace();

                            err.set(true);
                            stop.set(true);
                        }
                    }
                });

                threads.add(thread);

                thread.start();
            }

            for (int i = 0; i < UPDATE_THREADS; i++) {
                final int id = i;

                Thread thread;

                if (REMOVES) {
                    thread = new Thread(new Runnable() {
                        @Override public void run() {
                            try {
                                Thread.currentThread().setName("update" + id);

                                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                                while (!stop.get()) {
                                    int id1 = rnd.nextInt(ACCOUNTS);

                                    int id2 = rnd.nextInt(ACCOUNTS);

                                    while (id2 == id1)
                                        id2 = rnd.nextInt(ACCOUNTS);

                                    if (rnd.nextBoolean()) {
                                        //cluster.txRemoveTransfer(id1, id2);
                                    }
                                    else
                                        cluster.txTransfer(id1, id2, rnd.nextBoolean());
                                }
                            }
                            catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    });
                }
                else {
                    thread = new Thread(new Runnable() {
                        @Override public void run() {
                            try {
                                Thread.currentThread().setName("update" + id);

                                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                                while (!stop.get()) {
                                    int id1 = rnd.nextInt(ACCOUNTS);

                                    int id2 = rnd.nextInt(ACCOUNTS);

                                    while (id2 == id1)
                                        id2 = rnd.nextInt(ACCOUNTS);

                                    if (id1 > id2) {
                                        int tmp = id1;
                                        id1 = id2;
                                        id2 = tmp;
                                    }

                                    cluster.txTransfer(id1, id2, rnd.nextBoolean());
                                }
                            }
                            catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    });
                }

                threads.add(thread);

                thread.start();
            }

            long endTime = System.currentTimeMillis() + 2_000;

            while (!stop.get()) {
                Thread.sleep(1000);

                if (System.currentTimeMillis() >= endTime)
                    break;

                //cluster.dumpMvccInfo();
            }

            stop.set(true);

            for (Thread thread : threads)
                thread.join();

            Map<Object, Object> qryData = SQL ? cluster.sqlAll() : cluster.getAll(data.keySet());

            int sum = 0;

            for (int i = 0; i < ACCOUNTS; i++) {
                Integer val = (Integer)qryData.get(i);

                System.out.println("Val " + val);

                if (val != null)
                    sum += val;
            }

            System.out.println("Sum=" + sum + ", expSum=" + (ACCOUNTS * START_VAL));

            if (err.get()) {
                System.out.println("Error!");

                System.exit(1);
            }

//            cluster.dumpMvccInfo();
//
//            System.out.println("Cleanup");
//
//            cluster.cleanup();
//
//            cluster.dumpMvccInfo();

            TestDebugLog.clear();
        }
    }

    /**
     *
     */
    static class TestCluster {
        /** */
        final List<Node> nodes = new ArrayList<>();

        /** */
        final Coordinator crd;

        /** */
        final AtomicLong txIdGen = new AtomicLong(10_000);

        TestCluster(int nodesNum) {
            crd = new Coordinator();

            for (int i = 0; i < nodesNum; i++)
                nodes.add(new Node(i));
        }

        void cleanup() {
            CoordinatorCounter cntr = crd.cleanupVersion();

            for (Node node : nodes)
                node.dataStore.cleanup(cntr);
        }

        void txPutAll(Map<Object, Object> data) {
            TxId txId = new TxId(txIdGen.incrementAndGet());

            Map<Object, Node> mappedEntries = new LinkedHashMap<>();

            for (Object key : data.keySet()) {
                int nodeIdx = nodeForKey(key);

                Node node = nodes.get(nodeIdx);

                node.dataStore.lockEntry(key);

                mappedEntries.put(key, node);
            }

            TxVersion ver = crd.nextTxCounter(txId);

            MvccUpdateVersion mvccVer = new MvccUpdateVersion(ver.cntr, txId);

            for (Map.Entry<Object, Node> e : mappedEntries.entrySet()) {
                Node node = e.getValue();

                node.dataStore.updateEntry(e.getKey(), data.get(e.getKey()), mvccVer);
            }

            for (Map.Entry<Object, Node> e : mappedEntries.entrySet()) {
                Node node = e.getValue();

                node.dataStore.unlockEntry(e.getKey());
            }

            crd.txDone(txId, ver.cntr.cntr);
        }

        void txTransfer(Integer id1, Integer id2, boolean fromFirst) throws Exception {
            TreeSet<Integer> keys = new TreeSet<>();

            keys.add(id1);
            keys.add(id2);

            TxId txId = new TxId(txIdGen.incrementAndGet());

            Map<Object, Node> mappedEntries = new LinkedHashMap<>();

            Map<Object, Object> vals = new HashMap<>();

            for (Object key : keys) {
                int nodeIdx = nodeForKey(key);

                Node node = nodes.get(nodeIdx);

                node.dataStore.lockEntry(key);

                vals.put(key, node.dataStore.lastValue(key));

                mappedEntries.put(key, node);
            }

            TxVersion ver = crd.nextTxCounter(txId);

            Collection<TxId> waitTxs = null;

            for (Object key : keys) {
                int nodeIdx = nodeForKey(key);

                Node node = nodes.get(nodeIdx);

                Collection<TxId> txs = node.dataStore.waitTxsAck(key, ver.activeTxs);

                if (txs != null) {
                    if (waitTxs == null)
                        waitTxs = txs;
                    else
                        waitTxs.addAll(txs);
                }
            }

            if (waitTxs != null) {
                crd.waitTxs(waitTxs);
            }


            Integer curVal1 = (Integer)vals.get(id1);
            Integer curVal2 = (Integer)vals.get(id2);

            boolean update = false;

            Integer newVal1 = null;
            Integer newVal2 = null;

            if (curVal1 != null && curVal2 != null) {
                if (fromFirst) {
                    if (curVal1 > 0) {
                        update = true;

                        newVal1 = curVal1 - 1;
                        newVal2 = curVal2 + 1;
                    }
                }
                else {
                    if (curVal2 > 0) {
                        update = true;

                        newVal1 = curVal1 + 1;
                        newVal2 = curVal2 - 1;
                    }
                }
            }

            if (update) {
                Map<Object, Object> newVals = new HashMap<>();

                newVals.put(id1, newVal1);
                newVals.put(id2, newVal2);

                MvccUpdateVersion mvccVer = new MvccUpdateVersion(ver.cntr, txId);

                if (DEBUG_LOG) {
                    TestDebugLog.msgs.add(new TestDebugLog.Msg6("update", txId, id1, newVal1, id2, newVal2, ver.cntr));
                }

                for (Map.Entry<Object, Node> e : mappedEntries.entrySet()) {
                    Node node = e.getValue();

                    node.dataStore.updateEntry(e.getKey(), newVals.get(e.getKey()), mvccVer);
                }

                for (Map.Entry<Object, Node> e : mappedEntries.entrySet()) {
                    Node node = e.getValue();

                    node.dataStore.unlockEntry(e.getKey());
                }

                crd.txDone(txId, ver.cntr.cntr);
            }
            else {
                for (Map.Entry<Object, Node> e : mappedEntries.entrySet()) {
                    Node node = e.getValue();

                    node.dataStore.unlockEntry(e.getKey());
                }

                crd.txDone(txId, ver.cntr.cntr);
            }

//            if (DEBUG_LOG)
//                TestDebugLog.msgs.add(new TestDebugLog.Msg2("tx done", txId, cntr.cntr));
        }

//        void txRemoveTransfer(Integer from, Integer to) {
//            TreeSet<Integer> keys = new TreeSet<>();
//
//            keys.add(from);
//            keys.add(to);
//
//            TxId txId = new TxId(txIdGen.incrementAndGet());
//
//            Map<Object, Node> mappedEntries = new LinkedHashMap<>();
//
//            Map<Object, Object> vals = new HashMap<>();
//
//            for (Object key : keys) {
//                int nodeIdx = nodeForKey(key);
//
//                Node node = nodes.get(nodeIdx);
//
//                node.dataStore.lockEntry(key);
//
//                vals.put(key, node.dataStore.lastValue(key));
//
//                mappedEntries.put(key, node);
//            }
//
//            CoordinatorCounter cntr = crd.nextTxCounter(txId);
//
//            Integer fromVal = (Integer)vals.get(from);
//            Integer toVal = (Integer)vals.get(to);
//
//            boolean update = fromVal != null && toVal != null;
//
//            if (update) {
//                Map<Object, Object> newVals = new HashMap<>();
//
//                newVals.put(from, null);
//                newVals.put(to, fromVal + toVal);
//
//                MvccUpdateVersion mvccVer = new MvccUpdateVersion(cntr, txId);
//
//                if (DEBUG_LOG) {
//                    TestDebugLog.msgs.add(new TestDebugLog.Msg6("remove", txId, from, fromVal, to, toVal, cntr));
//                }
//
//                for (Map.Entry<Object, Node> e : mappedEntries.entrySet()) {
//                    Node node = e.getValue();
//
//                    node.dataStore.updateEntry(e.getKey(), newVals.get(e.getKey()), mvccVer);
//                }
//
//                for (Map.Entry<Object, Node> e : mappedEntries.entrySet()) {
//                    Node node = e.getValue();
//
//                    node.dataStore.unlockEntry(e.getKey());
//                }
//            }
//            else {
//                for (Map.Entry<Object, Node> e : mappedEntries.entrySet()) {
//                    Node node = e.getValue();
//
//                    node.dataStore.unlockEntry(e.getKey());
//                }
//            }
//
//            crd.txDone(txId, cntr.cntr);
//
//            if (DEBUG_LOG)
//                TestDebugLog.msgs.add(new TestDebugLog.Msg2("tx done", txId, cntr.cntr));
//        }

        public void dumpMvccInfo() {
            for (Node node : nodes) {
                int sql = node.dataStore.mvccSqlIdx.size();

                for (Map.Entry<Object, MvccValue> e : node.dataStore.mainIdx.entrySet()) {
                    List<MvccValue> list = node.dataStore.mvccIdx.get(e.getKey());

                    int size = 0;

                    if (list != null) {
                        synchronized (list) {
                            size = list.size();
                        }
                    }

                    System.out.println("Mvcc info [key=" + e.getKey() +
                        ", val=" + e.getValue() +
                        ", mvccVals=" + size +
                        ", sqlVals=" + sql + ']');
                }
            }
        }

        public Map<Object, Object> sqlAll() {
            MvccQueryVersion qryVer = crd.queryVersion();

            Map<Object, Object> res = new HashMap<>();

            for (Node node : nodes) {
                Map<Object, Object> nodeRes = node.dataStore.sqlQuery(qryVer);

                res.putAll(nodeRes);
            }

            crd.queryDone(qryVer.cntr);

            if (DEBUG_LOG) {
                TestDebugLog.msgs.add(new TestDebugLog.Msg3("sqlAll", qryVer.cntr, qryVer.activeTxs, res));
            }

            return res;
        }

        public Map<Object, Object> getAll(Set<?> keys) {
            MvccQueryVersion qryVer = crd.queryVersion();

            Map<Object, Object> res = new HashMap<>();

            for (Object key : keys) {
                int nodeIdx = nodeForKey(key);

                Node node = nodes.get(nodeIdx);

                Object val = node.dataStore.get(key, qryVer);

                res.put(key, val);
            }

            crd.queryDone(qryVer.cntr);

            if (DEBUG_LOG) {
                TestDebugLog.msgs.add(new TestDebugLog.Msg3("getAll", qryVer.cntr, qryVer.activeTxs, res));
            }

            return res;
        }

        private int nodeForKey(Object key) {
            return U.safeAbs(key.hashCode()) % nodes.size();
        }
    }

    /**
     *
     */
    static class Node {
        /** */
        final DataStore dataStore;

        /** */
        final int nodexIdx;

        public Node(int nodexIdx) {
            this.nodexIdx = nodexIdx;

            dataStore = new DataStore();
        }

        @Override public String toString() {
            return "Node [idx=" + nodexIdx + ']';
        }
    }

    static class TxVersion {
        final CoordinatorCounter cntr;

        /** */
        @GridToStringInclude
        final Collection<TxId> activeTxs;

        public TxVersion(CoordinatorCounter cntr, Collection<TxId> activeTxs) {
            this.cntr = cntr;
            this.activeTxs = activeTxs;
        }
    }

    /**
     *
     */
    static class Coordinator {
        /** */
        private final AtomicLong cntr = new AtomicLong(-1);

        /** */
        private final GridAtomicLong commitCntr = new GridAtomicLong(-1);

        /** */
        private final Map<Long, Integer> activeQueries = new ConcurrentHashMap8<>();

        /** */
        @GridToStringInclude
        private final ConcurrentHashMap8<TxId, Long> activeTxs = new ConcurrentHashMap8<>();

        synchronized void waitTxs(Collection<TxId> waitTxs) throws InterruptedException {
            for (TxId txId : waitTxs) {
                while (activeTxs.containsKey(txId))
                    wait();
            }
        }

        synchronized TxVersion nextTxCounter(TxId txId) {
            long cur = cntr.get();

            activeTxs.put(txId, cur + 1);

            CoordinatorCounter newCtr = new CoordinatorCounter(cntr.incrementAndGet());

            Set<TxId> txs = new HashSet<>();

            for (Map.Entry<TxId, Long> e : activeTxs.entrySet())
                txs.add(e.getKey());

            TxVersion ver = new TxVersion(newCtr, txs);

            return ver;
        }

        synchronized void txDone(TxId txId, long cntr) {
            Long rmvd = activeTxs.remove(txId);

            assert rmvd != null;

            commitCntr.setIfGreater(cntr);

            notifyAll();
        }

        private Long minActive(Set<TxId> txs) {
            Long minActive = null;

            for (Map.Entry<TxId, Long> e : activeTxs.entrySet()) {
                if (txs != null)
                    txs.add(e.getKey());

//                TxId val = e.getValue();
//
//                while (val.cntr == -1)
//                    Thread.yield();

                long cntr = e.getValue();

                if (minActive == null)
                    minActive = cntr;
                else if (cntr < minActive)
                    minActive = cntr;
            }

            return minActive;
        }

        synchronized MvccQueryVersion queryVersion() {
            long useCntr = commitCntr.get();

//            Long minActive = minActive(txs);
//
//            if (minActive != null && minActive < useCntr)
//                useCntr = minActive - 1;

            Set<TxId> txs = new HashSet<>();

            for (Map.Entry<TxId, Long> e : activeTxs.entrySet())
                txs.add(e.getKey());

            MvccQueryVersion qryVer = new MvccQueryVersion(new CoordinatorCounter(useCntr), txs);

            Integer qryCnt = activeQueries.get(useCntr);

            if (qryCnt != null)
                activeQueries.put(useCntr, qryCnt + 1);
            else
                activeQueries.put(useCntr, 1);


            return qryVer;
        }

        synchronized void queryDone(CoordinatorCounter cntr) {
            Integer qryCnt = activeQueries.get(cntr.cntr);

            assert qryCnt != null : cntr.cntr;

            int left = qryCnt - 1;

            assert left >= 0 : left;

            if (left == 0)
                activeQueries.remove(cntr.cntr);
        }

        synchronized CoordinatorCounter cleanupVersion() {
            long useCntr = commitCntr.get();

            Long minActive = minActive(null);

            if (minActive != null && minActive < useCntr)
                useCntr = minActive - 1;

            for (Long qryCntr : activeQueries.keySet()) {
                if (qryCntr <= useCntr)
                    useCntr = qryCntr - 1;
            }

            return new CoordinatorCounter(useCntr);
        }

        @Override public String toString() {
            return S.toString(Coordinator.class, this);
        }
    }

    /**
     *
     */
    static class CoordinatorCounter implements Comparable<CoordinatorCounter> {
        /** */
        private final long topVer; // TODO

        /** */
        private final long cntr;

        CoordinatorCounter(long cntr) {
            this.topVer = 1;
            this.cntr = cntr;
        }

        @Override public int compareTo(CoordinatorCounter o) {
            return Long.compare(cntr, o.cntr);
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            CoordinatorCounter that = (CoordinatorCounter)o;

            return cntr == that.cntr;
        }

        @Override public int hashCode() {
            return (int)(cntr ^ (cntr >>> 32));
        }

        @Override public String toString() {
            return "Cntr [c=" + cntr + ']';
        }
    }

    /**
     *
     */
    static class MvccUpdateVersion {
        /** */
        @GridToStringInclude
        final CoordinatorCounter cntr;

        /** */
        @GridToStringInclude
        final TxId txId;

        /**
         * @param cntr
         */
        MvccUpdateVersion(CoordinatorCounter cntr, TxId txId) {
            assert cntr != null;

            this.cntr = cntr;
            this.txId = txId;
        }

        @Override public String toString() {
            return S.toString(MvccUpdateVersion.class, this);
        }
    }

    /**
     *
     */
    static class MvccQueryVersion {
        /** */
        @GridToStringInclude
        final CoordinatorCounter cntr;

        /** */
        @GridToStringInclude
        final Collection<TxId> activeTxs;

        MvccQueryVersion(CoordinatorCounter cntr, Collection<TxId> activeTxs) {
            this.cntr = cntr;
            this.activeTxs = activeTxs;
        }

        @Override public String toString() {
            return S.toString(MvccQueryVersion.class, this);
        }
    }

    /**
     *
     */
    static class TxId {
        /** */
        @GridToStringInclude
        final long id;

        TxId(long id) {
            this.id = id;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TxId txId = (TxId) o;

            return id == txId.id;
        }

        @Override public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }

        @Override public String toString() {
            return S.toString(TxId.class, this);
        }
    }

    /**
     *
     */
    static class SqlKey implements Comparable<SqlKey> {
        /** */
        final Comparable key;

        /** */
        final Comparable val;

        /** */
        final CoordinatorCounter cntr;

        public SqlKey(Object key, Object val, CoordinatorCounter cntr) {
            this.key = (Comparable)key;
            this.val = (Comparable)val;
            this.cntr = cntr;
        }

        @Override public int compareTo(@NotNull SqlKey o) {
            int cmp;

            if (val != null && o.val != null)
                cmp = val.compareTo(o.val);
            else {
                if (val != null)
                    cmp = 1;
                else
                    cmp = o.val == null ? 0 : -1;
            }


            if (cmp == 0) {
                cmp = key.compareTo(o.key);

                if (cmp == 0)
                    cmp = cntr.compareTo(o.cntr);
            }

            return cmp;
        }

        @Override public String toString() {
            return "SqlKey [key=" + key + ", val=" + val + ']';
        }
    }

    /**
     *
     */
    static class DataStore {
        /** */
        private final ConcurrentHashMap<Object, ReentrantLock> locks = new ConcurrentHashMap<>();

        /** */
        final ConcurrentHashMap<Object, MvccValue> mainIdx = new ConcurrentHashMap<>();

        /** */
        final ConcurrentHashMap<Object, List<MvccValue>> mvccIdx = new ConcurrentHashMap<>();

        /** */
        final ConcurrentSkipListMap<SqlKey, MvccSqlValue> mvccSqlIdx = new ConcurrentSkipListMap<>();

        void cleanup(CoordinatorCounter cleanupCntr) {
            for (Map.Entry<Object, List<MvccValue>> e : mvccIdx.entrySet()) {
                lockEntry(e.getKey());

                try {
                    List<MvccValue> list = e.getValue();

                    synchronized (list) {
                        for (int i = list.size() - 1; i >= 0; i--) {
                            MvccValue val = list.get(i);

                            if (val.ver.cntr.compareTo(cleanupCntr) <= 0) {
                                if (DEBUG_LOG) {
                                    TestDebugLog.msgs.add(new TestDebugLog.Msg6_1("cleanup",
                                        e.getKey(), val.val, val.ver, cleanupCntr.cntr, null, null));
                                }

                                MvccValue prev;

                                if (val.val != null)
                                    prev = mainIdx.put(e.getKey(), val);
                                else
                                    prev = mainIdx.remove(e.getKey());

                                if (prev != null) {
                                    SqlKey key = new SqlKey(e.getKey(), prev.val, prev.ver.cntr);

                                    MvccSqlValue old = mvccSqlIdx.remove(key);

                                    assert old != null;
                                }

                                for (int j = 0; j <= i; j++) {
                                    MvccValue rmvd = list.remove(0);

                                    assert rmvd != null;

                                    if (j != i || rmvd.val == null) {
                                        SqlKey key = new SqlKey(e.getKey(), rmvd.val, rmvd.ver.cntr);

                                        MvccSqlValue old = mvccSqlIdx.remove(key);

                                        assert old != null;
                                    }
                                }

                                if (list.isEmpty())
                                    mvccIdx.remove(e.getKey());

                                break;
                            }
                        }
                    }
                }
                finally {
                    unlockEntry(e.getKey());
                }
            }
        }

        Collection<TxId> waitTxsAck(Object key, Collection<TxId> activeTxs) {
            if (!F.isEmpty(activeTxs))
                return null;

            List<MvccValue> list = mvccIdx.get(key);

            List<TxId> waitTxs = null;

            if (list != null) {
                for (MvccValue val : list) {
                    if (activeTxs.contains(val.ver.txId)) {
                        if (waitTxs == null)
                            waitTxs = new ArrayList<>();

                        waitTxs.add(val.ver.txId);
                    }
                }
            }

            return waitTxs;
        }

        void lockEntry(Object key) {
            ReentrantLock e = lock(key);

            e.lock();
        }

        void unlockEntry(Object key) {
            ReentrantLock e = lock(key);

            e.unlock();
        }

        void updateEntry(Object key, Object val, MvccUpdateVersion ver) {
            List<MvccValue> list = mvccIdx.get(key);

            if (list == null) {
                Object old = mvccIdx.putIfAbsent(key, list = new ArrayList<>());

                assert old == null;
            }

            MvccValue prevVal = null;

            synchronized (list) {
                if (!list.isEmpty())
                    prevVal = list.get(list.size() - 1);

                list.add(new MvccValue(val, ver));
            }

            if (prevVal == null)
                prevVal = mainIdx.get(key);

            if (prevVal != null) {
                SqlKey prevKey = new SqlKey(key, prevVal.val, prevVal.ver.cntr);

                MvccSqlValue old =
                    mvccSqlIdx.put(prevKey, new MvccSqlValue(prevVal.val, prevVal.ver, ver));

                assert old != null;
            }

            mvccSqlIdx.put(new SqlKey(key, val, ver.cntr), new MvccSqlValue(val, ver, null));
        }

        Object lastValue(Object key) {
            List<MvccValue> list = mvccIdx.get(key);

            if (list != null) {
                synchronized (list) {
                    if (list.size() > 0)
                        return list.get(list.size() - 1).val;
                }
            }

            MvccValue val = mainIdx.get(key);

            return val != null ? val.val : null;
        }

        Map<Object, Object> sqlQuery(MvccQueryVersion qryVer) {
            Map<Object, Object> res = new HashMap<>();

            for (Map.Entry<SqlKey, MvccSqlValue> e : mvccSqlIdx.entrySet()) {
                MvccSqlValue val = e.getValue();

                if (!versionVisible(val.ver, qryVer)) {
                    if (DEBUG_LOG) {
                        TestDebugLog.msgs.add(new TestDebugLog.Msg3("sql skip mvcc val", e.getKey().key, val.val, val.ver));
                    }

                    continue;
                }

                MvccUpdateVersion newVer = val.newVer;

                if (newVer != null && versionVisible(newVer, qryVer)) {
                    if (DEBUG_LOG) {
                        TestDebugLog.msgs.add(new TestDebugLog.Msg4("sql skip mvcc val2", e.getKey().key, val.val, val.ver, val.newVer));
                    }

                    continue;
                }

                Object old = res.put(e.getKey().key, e.getValue().val);

                if (DEBUG_LOG) {
                    //TestDebugLog.msgs.add(new TestDebugLog.Msg4("sql get mvcc val", e.getKey().key, val.val, val.ver, val.newVer));
                }

                if (old != null) {
                    TestDebugLog.printAllAndExit("Already has value for key [key=" + e.getKey().key +
                        ", qryVer=" + qryVer +
                        ", oldVal=" + old +
                        ", newVal=" + e.getValue().val +
                        ']');
                }

                assert old == null;
            }

            return res;
        }

        private boolean versionVisible(MvccUpdateVersion ver, MvccQueryVersion qryVer) {
            int cmp = ver.cntr.compareTo(qryVer.cntr);

            return cmp <= 0 && !qryVer.activeTxs.contains(ver.txId);
        }

        Object get(Object key, MvccQueryVersion ver) {
            List<MvccValue> list = mvccIdx.get(key);

            if (list != null) {
                synchronized (list) {
                    for (int i = list.size() - 1; i >= 0; i--) {
                        MvccValue val = list.get(i);

                        if (!versionVisible(val.ver, ver))
                            continue;

                        if (DEBUG_LOG) {
                            TestDebugLog.msgs.add(new TestDebugLog.Msg3("read mvcc val", key, val.val, val.ver));
                        }

                        return val.val;
                    }
                }
            }

            MvccValue val = mainIdx.get(key);

            if (val != null) {
                int cmp = val.ver.cntr.compareTo(ver.cntr);

                if (DEBUG_LOG) {
                    if (cmp > 0) {
                        synchronized (TestDebugLog.msgs) {
                            TestDebugLog.msgs.add(new TestDebugLog.Message("Committed [key=" + key + ", ver=" + val.ver + ", qryVer=" + ver.cntr + ']'));

                            TestDebugLog.printAllAndExit("Committed [key=" + key + ", ver=" + val.ver + ", qryVer=" + ver + ']');
                        }
                    }
                }

                assert cmp <= 0 : "Committed [ver=" + val.ver + ", qryVer=" + ver.cntr + ']';

                if (DEBUG_LOG)
                    TestDebugLog.msgs.add(new TestDebugLog.Msg3("read comitted val", key, val, val.ver));
            }
            else {
                if (DEBUG_LOG)
                    TestDebugLog.msgs.add(new TestDebugLog.Msg3("read comitted null", key, null, null));
            }

            return val != null ? val.val : null;
        }

        private ReentrantLock lock(Object key) {
            ReentrantLock e = locks.get(key);

            if (e == null) {
                ReentrantLock old = locks.putIfAbsent(key, e = new ReentrantLock());

                if (old != null)
                    e = old;
            }

            return e;
        }
    }

    /**
     *
     */
    static class MvccValue {
        /** */
        @GridToStringInclude
        final Object val;

        /** */
        @GridToStringInclude
        final MvccUpdateVersion ver;

        MvccValue(Object val, MvccUpdateVersion ver) {
            assert ver != null;

            this.val = val;
            this.ver = ver;
        }

        @Override public String toString() {
            return S.toString(MvccValue.class, this);
        }
    }

    /**
     *
     */
    static class MvccSqlValue {
        /** */
        @GridToStringInclude
        final Object val;

        /** */
        @GridToStringInclude
        final MvccUpdateVersion ver;

        /** */
        @GridToStringInclude
        final MvccUpdateVersion newVer;

        MvccSqlValue(Object val, MvccUpdateVersion ver, MvccUpdateVersion newVer) {
            assert ver != null;

            this.val = val;
            this.ver = ver;
            this.newVer = newVer;
        }

        @Override public String toString() {
            return S.toString(MvccSqlValue.class, this);
        }
    }

    static void log(String msg) {
        System.out.println(Thread.currentThread() + ": " + msg);
    }

    static class TestDebugLog {
        /** */
        //static final List<Object> msgs = Collections.synchronizedList(new ArrayList<>(1_000_000));
        static final ConcurrentLinkedQueue<Object> msgs = new ConcurrentLinkedQueue<>();



        /** */
        private static final SimpleDateFormat DEBUG_DATE_FMT = new SimpleDateFormat("HH:mm:ss,SSS");

        static class Message {
            String thread = Thread.currentThread().getName();

            String msg;

            long ts = U.currentTimeMillis();

            public Message(String msg) {
                this.msg = msg;
            }

            public String toString() {
                return "Msg [msg=" + msg + ", thread=" + thread + ", time=" + DEBUG_DATE_FMT.format(new Date(ts)) + ']';
            }
        }

        static class Msg2 extends Message{
            Object v1;
            Object v2;

            public Msg2(String msg, Object v1, Object v2) {
                super(msg);
                this.v1 = v1;
                this.v2 = v2;
            }
            public String toString() {
                return "Msg [msg=" + msg +
                    ", v1=" + v1 +
                    ", v2=" + v2 +
                    ", msg=" + msg +
                    ", thread=" + thread +
                    ", time=" + DEBUG_DATE_FMT.format(new Date(ts)) + ']';
            }
        }

        static class Msg3 extends Message{
            Object v1;
            Object v2;
            Object v3;

            public Msg3(String msg, Object v1, Object v2, Object v3) {
                super(msg);
                this.v1 = v1;
                this.v2 = v2;
                this.v3 = v3;
            }
            public String toString() {
                return "Msg [msg=" + msg +
                    ", v1=" + v1 +
                    ", v2=" + v2 +
                    ", v3=" + v3 +
                    ", thread=" + thread +
                    ", time=" + DEBUG_DATE_FMT.format(new Date(ts)) + ']';
            }
        }

        static class Msg4 extends Message{
            Object v1;
            Object v2;
            Object v3;
            Object v4;

            public Msg4(String msg, Object v1, Object v2, Object v3, Object v4) {
                super(msg);
                this.v1 = v1;
                this.v2 = v2;
                this.v3 = v3;
                this.v4 = v4;
            }

            public String toString() {
                return "Msg [msg=" + msg +
                    ", v1=" + v1 +
                    ", v2=" + v2 +
                    ", v3=" + v3 +
                    ", v4=" + v4 +
                    ", thread=" + thread +
                    ", time=" + DEBUG_DATE_FMT.format(new Date(ts)) + ']';
            }
        }

        static class Msg6 extends Message{
            Object v1;
            Object v2;
            Object v3;
            Object v4;
            Object v5;
            Object v6;

            public Msg6(String msg, Object v1, Object v2, Object v3, Object v4, Object v5, Object v6) {
                super(msg);
                this.v1 = v1;
                this.v2 = v2;
                this.v3 = v3;
                this.v4 = v4;
                this.v5 = v5;
                this.v6 = v6;
            }

            public String toString() {
                return "Msg [msg=" + msg +
                    ", txId=" + v1 +
                    ", id1=" + v2 +
                    ", v1=" + v3 +
                    ", id2=" + v4 +
                    ", v2=" + v5 +
                    ", cntr=" + v6 +
                    ", thread=" + thread +
                    ", time=" + DEBUG_DATE_FMT.format(new Date(ts)) + ']';
            }
        }
        static class Msg6_1 extends Message{
            Object v1;
            Object v2;
            Object v3;
            Object v4;
            Object v5;
            Object v6;

            public Msg6_1(String msg, Object v1, Object v2, Object v3, Object v4, Object v5, Object v6) {
                super(msg);
                this.v1 = v1;
                this.v2 = v2;
                this.v3 = v3;
                this.v4 = v4;
                this.v5 = v5;
                this.v6 = v6;
            }

            public String toString() {
                return "Msg [msg=" + msg +
                    ", key=" + v1 +
                    ", val=" + v2 +
                    ", ver=" + v3 +
                    ", cleanupC=" + v4 +
                    ", thread=" + thread +
                    ", time=" + DEBUG_DATE_FMT.format(new Date(ts)) + ']';
            }
        }

        static class EntryMessage extends Message {
            Object key;
            Object val;

            public EntryMessage(Object key, Object val, String msg) {
                super(msg);

                this.key = key;
                this.val = val;
            }

            public String toString() {
                return "EntryMsg [key=" + key + ", val=" + val + ", msg=" + msg + ", thread=" + thread + ", time=" + DEBUG_DATE_FMT.format(new Date(ts)) + ']';
            }
        }

        static class PartMessage extends Message {
            int p;
            Object val;

            public PartMessage(int p, Object val, String msg) {
                super(msg);

                this.p = p;
                this.val = val;
            }

            public String toString() {
                return "PartMessage [p=" + p + ", val=" + val + ", msg=" + msg + ", thread=" + thread + ", time=" + DEBUG_DATE_FMT.format(new Date(ts)) + ']';
            }
        }

        static final boolean out = false;

        public static void addMessage(String msg) {
            msgs.add(new Message(msg));

            if (out)
                System.out.println(msg);
        }

        public static void addEntryMessage(Object key, Object val, String msg) {
            if (key instanceof KeyCacheObject)
                key = ((KeyCacheObject)key).value(null, false);

            EntryMessage msg0 = new EntryMessage(key, val, msg);

            msgs.add(msg0);

            if (out) {
                System.out.println(msg0.toString());

                System.out.flush();
            }
        }

        public static void addPartMessage(int p, Object val, String msg) {
            PartMessage msg0 = new PartMessage(p, val, msg);

            msgs.add(msg0);

            if (out) {
                System.out.println(msg0.toString());

                System.out.flush();
            }
        }

        static void printAllAndExit(String msg) {
            System.out.println(msg);

            TestDebugLog.addMessage(msg);

            List<Object> msgs = TestDebugLog.printMessages(true, null);

            TestDebugLog.printMessages0(msgs, "test_debug_update.txt");

            TestDebugLog.printMessagesForThread(msgs, Thread.currentThread().getName(), "test_debug_thread.txt");

            System.exit(1);
        }

        public static void printMessagesForThread(List<Object> msgs0, String thread0, String file) {
            try {
                FileOutputStream out = new FileOutputStream(file);

                PrintWriter w = new PrintWriter(out);

                for (Object msg : msgs0) {
                    if (msg instanceof Message) {
                        String thread = ((Message) msg).thread;

                        if (thread.equals(thread0))
                            w.println(msg.toString());
                    }
                }

                w.close();

                out.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }

        public static void printMessages0(List<Object> msgs0, String file) {
            try {
                FileOutputStream out = new FileOutputStream(file);

                PrintWriter w = new PrintWriter(out);

                for (Object msg : msgs0) {
                    if (msg instanceof Message) {
                        String msg0 = ((Message) msg).msg;

                        if (msg0.equals("tx done") || msg0.equals("update") || msg0.equals("cleanup"))
                            w.println(msg.toString());
                    }
                }

                w.close();

                out.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }

        public static List<Object> printMessages(boolean file, Integer part) {
            List<Object> msgs0;

            synchronized (msgs) {
                msgs0 = new ArrayList<>(msgs);

                msgs.clear();
            }

            if (file) {
                try {
                    FileOutputStream out = new FileOutputStream("test_debug.log");

                    PrintWriter w = new PrintWriter(out);

                    for (Object msg : msgs0) {
                        if (part != null && msg instanceof PartMessage) {
                            if (((PartMessage) msg).p != part)
                                continue;
                        }

                        w.println(msg.toString());
                    }

                    w.close();

                    out.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else {
                for (Object msg : msgs0)
                    System.out.println(msg);
            }

            return msgs0;
        }

        public static void printKeyMessages(boolean file, Object key) {
            List<Object> msgs0;

            synchronized (msgs) {
                msgs0 = new ArrayList<>(msgs);

                msgs.clear();
            }

            if (file) {
                try {
                    FileOutputStream out = new FileOutputStream("test_debug.log");

                    PrintWriter w = new PrintWriter(out);

                    for (Object msg : msgs0) {
                        if (msg instanceof EntryMessage && !((EntryMessage)msg).key.equals(key))
                            continue;

                        w.println(msg.toString());
                    }

                    w.close();

                    out.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else {
                for (Object msg : msgs0) {
                    if (msg instanceof EntryMessage && !((EntryMessage)msg).key.equals(key))
                        continue;

                    System.out.println(msg);
                }
            }
        }

        public static void clear() {
            msgs.clear();
        }

        public static void clearEntries() {
            for (Iterator it = msgs.iterator(); it.hasNext();) {
                Object msg = it.next();

                if (msg instanceof EntryMessage)
                    it.remove();
            }
        }

    }}
