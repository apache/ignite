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

package org.apache.ignite.internal.processors.cache.consistentcut;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.thread.IgniteThreadFactory;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

/** */
public abstract class AbstractConsistentCutBlockingTest extends AbstractConsistentCutTest {
    /** */
    protected int caseNum;

    /** */
    protected static volatile CountDownLatch txLatch;

    /** */
    protected static volatile CountDownLatch cutLatch;

    /** */
    protected final Map<IgniteUuid, Integer> txOrigNode = new ConcurrentHashMap<>();

    /** Use cached thread pool executor, because tests may run > 100 tests cases (every case spawns a thread for running tx). */
    protected static final ExecutorService exec = Executors.newCachedThreadPool(
        new IgniteThreadFactory("ConsistentCutTest", "consistent-cut-blk-test"));

    /** */
    protected void runCases(List<List<T2<Integer, Integer>>> cases) throws Exception {
        // Ignite coordinator, ordinary server node, client node.
        Set<Integer> nears = F.asSet(0, nodes() - 1, nodes());

        for (int near: nears) {
            for (int cs = 0; cs < cases.size(); cs++) {
                final int n = near;
                final int c = cs;

                runCase(() -> tx(n, cases.get(c)), near, cases.get(c));
            }
        }
    }

    /**
     * @param tx Function that performs transaction.
     * @param nearNodeId ID of near node.
     * @param c Test case - list of tuples (prim, backup) to be written.
     */
    protected void runCase(Runnable tx, Integer nearNodeId, List<T2<Integer, Integer>> c) throws Exception {
        long prevVer = grid(0).context().cache().context().consistentCutMgr().latestCutVersion();

        cutLatch = new CountDownLatch(1);
        txLatch = new CountDownLatch(1);

        caseNum++;

        log.info("START CASE " + caseNum + ". Data=" + c + ", nearNodeId=" + nearNodeId);

        Future<?> txFut = exec.submit(() -> {
            tx.run();

            // Some cases don't block on blkMsgCls. Then no need to await txLatch for such cases.
            txLatch.countDown();
        });

        txLatch.await(1_000, TimeUnit.MILLISECONDS);

        grid(0).context().cache().context().consistentCutMgr().triggerConsistentCutOnCluster();

        awaitCutStarted(prevVer, null);

        cutLatch.countDown();

        txFut.get(getTestTimeout(), TimeUnit.MILLISECONDS);

        // Await while all async operations completed.
        awaitGlobalCutReady(prevVer);
    }

    /**
     * @param near ID of node to coordinate a transaction.
     * @param keys List of pairs { primary -> backup } for keys to participate in tx.
     */
    protected void tx(int near, List<T2<Integer, Integer>> keys) {
        try (Transaction tx = grid(near).transactions().txStart()) {
            txOrigNode.put(tx.xid(), near);

            for (T2<Integer, Integer> desc: keys) {
                int primary = desc.getKey();
                Integer backup = desc.getValue();

                ClusterNode backupNode = backup == null ? null : grid(backup).localNode();

                int key = key(CACHE, grid(primary).localNode(), backupNode);

                grid(near).cache(CACHE).put(key, key);
            }

            tx.commit();
        }
    }

    /**
     * Await Consistent Cut started on every node.
     *
     * @param prevCutVer Previous Consistent Cut version.
     * @param excl Node to exclude awaiting Consistent Cut starts.
     * @return Version of the latest Consistent Cut version.
     */
    protected long awaitCutStarted(long prevCutVer, @Nullable Integer excl) throws Exception {
        long newCutVer = -1L;

        Function<Integer, ConsistentCutManager> cutMgr = (n) -> grid(n).context().cache().context().consistentCutMgr();

        int starts = 0;

        // Wait Consistent Cut locally started on every node (prepared the check-list).
        for (int n = 0; n < nodes(); n++) {
            if (excl != null && n == excl) {
                if (++starts == nodes())
                    return newCutVer;

                continue;
            }

            for (int i = 0; i < 50; i++) {
                ConsistentCutState cutState = cutMgr.apply(n).latestCutState();

                long ver = cutState.version();

                if (ver > prevCutVer) {
                    if (newCutVer < 0)
                        newCutVer = ver;
                    else
                        assert newCutVer == ver : "new=" + newCutVer + ", rcv=" + ver + ", prev=" + prevCutVer;

                    if (++starts == nodes())
                        return newCutVer;

                    break;
                }

                Thread.sleep(10);
            }
        }

        StringBuilder bld = new StringBuilder()
            .append("Failed to wait Consitent Cut")
            .append(" newCutVer ").append(newCutVer)
            .append(", prevCutVer ").append(prevCutVer)
            .append(", excl node ").append(excl);

        for (int n = 0; n < nodes(); n++)
            bld.append("\nNode").append(n).append( ": ").append(cutMgr.apply(n).latestCutState());

        throw new Exception(bld.toString());
    }

    /** */
    protected enum NodeType {
        /** */
        NEAR,

        /** */
        PRIMARY,

        /** */
        BACKUP
    }
}
