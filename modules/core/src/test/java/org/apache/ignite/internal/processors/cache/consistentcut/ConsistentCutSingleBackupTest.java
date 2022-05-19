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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** */
@RunWith(Parameterized.class)
public class ConsistentCutSingleBackupTest extends AbstractConsistentCutBlockingTest {
    /** Count of server nodes to start. */
    protected static final int SRV_NODES = 2;

    /** */
    @Parameterized.Parameter()
    public int blkNodeId;

    /** */
    @Parameterized.Parameter(1)
    public static String blkMsgCls;

    /** */
    @Parameterized.Parameters(name = "nodeId={0}, m={1}")
    public static List<Object[]> params() {
        List<Object[]> p = new ArrayList<>();

        // Coordinator, ordinary server node, client node.
        Set<Integer> nodes = F.asSet(0, SRV_NODES - 1, SRV_NODES);

        // +1 - client node.
        for (int i: nodes) {
            for (String m: messages())
                p.add(new Object[] { i, m });
        }

        return p;
    }

    /** */
    public static List<String> messages() {
        List<String> msgCls = new ArrayList<>();

        msgCls.add(GridNearTxPrepareRequest.class.getSimpleName());
        msgCls.add(GridNearTxPrepareResponse.class.getSimpleName());
        msgCls.add(GridNearTxFinishRequest.class.getSimpleName());
        msgCls.add(GridNearTxPrepareResponse.class.getSimpleName());
        msgCls.add(GridDhtTxPrepareRequest.class.getSimpleName());
        msgCls.add(GridDhtTxPrepareResponse.class.getSimpleName());
        msgCls.add(GridDhtTxFinishRequest.class.getSimpleName());
        msgCls.add(GridDhtTxFinishResponse.class.getSimpleName());

        return msgCls;
    }

    /** */
    protected final Map<IgniteUuid, Integer> txOrigNode = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        blkMsgNode = grid(blkNodeId).localNode().id();
    }

    /** */
    @Test
    public void testMultipleCases() throws Exception {
        List<List<T2<Integer, Integer>>> cases = new ArrayList<>();

        // Possible pairs { primary -> backup }.
        List<T2<Integer, Integer>> pairs = new ArrayList<>();

        for (int p = 0; p < SRV_NODES; p++) {
            for (int b = 0; b < SRV_NODES; b++) {
                if (b == p)
                    continue;

                pairs.add(new T2<>(p, b));
            }
        }

        // for cnt = 1;
        for (int p = 0; p < pairs.size(); p++) {
            List<T2<Integer, Integer>> c = new ArrayList<>();
            c.add(pairs.get(p));

            cases.add(c);
        }

        // for cnt = 2;
        for (int p1 = 0; p1 < pairs.size(); p1++) {
            for (int p2 = 0; p2 < pairs.size(); p2++) {
                List<T2<Integer, Integer>> c = new ArrayList<>();

                c.add(pairs.get(p1));
                c.add(pairs.get(p2));

                cases.add(c);
            }
        }

        runCases(cases);

        checkWals(txOrigNode, caseCnt, caseCnt);
    }

    /** */
    protected void runCases(List<List<T2<Integer, Integer>>> cases) throws Exception {
        // Ignite coordinator, ordinary server node, client node.
        Set<Integer> nears = F.asSet(0, nodes() - 1, nodes());

        for (int near: nears) {
            for (int cs = 0; cs < cases.size(); cs++) {
                final int n = near;
                final int c = cs;

                runCase(() -> tx(n, cases.get(c)), cases.get(c));
            }
        }
    }

    /**
     * @param near ID of node to coordinate a transaction.
     * @param keys List of pairs { primary -> backup } for keys to participate in tx.
     */
    private void tx(int near, List<T2<Integer, Integer>> keys) {
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

    /** {@inheritDoc} */
    @Override protected int nodes() {
        return SRV_NODES;
    }

    /** {@inheritDoc} */
    @Override protected int backups() {
        return 1;
    }
}
