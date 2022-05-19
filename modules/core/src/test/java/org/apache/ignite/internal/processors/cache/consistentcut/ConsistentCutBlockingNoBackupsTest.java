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
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** */
@RunWith(Parameterized.class)
public class ConsistentCutBlockingNoBackupsTest extends AbstractConsistentCutBlockingTest {
    /** Count of server nodes to start. */
    private static final int SRV_NODES = 3;

    /** */
    @Parameterized.Parameter()
    public int blkNodeId;

    /** */
    @Parameterized.Parameters(name = "nodeId={0}")
    public static List<Object[]> params() {
        List<Object[]> p = new ArrayList<>();

        // +1 - client node.
        for (int i = 0; i < SRV_NODES + 1; i++)
            p.add(new Object[] { i });

        return p;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        blkMsgNode = grid(blkNodeId).localNode().id();
    }

    /** */
    private final Map<IgniteUuid, Integer> txOrigNode = new ConcurrentHashMap<>();

    /** */
    @Test
    public void testMultipleCases() throws Exception {
        for (String msg: messages()) {
            blkMsgCls = msg;

            caseClientSingleKey();
            caseClientMultipleKey();
            caseClientMultipleNodes();
            caseClientAllNodes();
            caseCoordinatorOnly();
            caseCoordinatorOtherNode();
            caseCoordinatorAllNodes();
            caseNonCoordinatorOnly();
            caseNonCoordinatorOtherNode();
            caseNonCoordinatorAllNodes();
        }

        checkWals(txOrigNode, caseCnt, caseCnt);
    }

    /** No backups then skip Dht messages. */
    public List<String> messages() {
        List<String> msgCls = new ArrayList<>();

        msgCls.add(GridNearTxPrepareRequest.class.getSimpleName());
        msgCls.add(GridNearTxPrepareResponse.class.getSimpleName());
        msgCls.add(GridNearTxFinishRequest.class.getSimpleName());
        msgCls.add(GridNearTxPrepareResponse.class.getSimpleName());

        return msgCls;
    }

    /**
     * Near = client
     * Primary = [0]
     */
    private void caseClientSingleKey() throws Exception {
        runCase(() -> {
            try (Transaction tx = clientNode().transactions().txStart()) {
                txOrigNode.put(tx.xid(), nodes());

                int key = key(CACHE, grid(0).localNode(), null);

                clientNode().cache(CACHE).put(key, key);

                tx.commit();
            }
        });
    }

    /**
     * Near = client
     * Primary = [0]
     */
    public void caseClientMultipleKey() throws Exception {
        runCase(() -> {
            try (Transaction tx = clientNode().transactions().txStart()) {
                txOrigNode.put(tx.xid(), nodes());

                int key1 = key(CACHE, grid(0).localNode(), null);
                int key2 = key(CACHE, grid(0).localNode(), null);

                clientNode().cache(CACHE).put(key1, key1);
                clientNode().cache(CACHE).put(key2, key2);

                tx.commit();
            }
        });
    }

    /**
     * Near = client
     * Primary = [0, 1]
     */
    public void caseClientMultipleNodes() throws Exception {
        runCase(() -> {
            try (Transaction tx = clientNode().transactions().txStart()) {
                txOrigNode.put(tx.xid(), nodes());

                int key1 = key(CACHE, grid(0).localNode(), null);
                int key2 = key(CACHE, grid(1).localNode(), null);

                clientNode().cache(CACHE).put(key1, key1);
                clientNode().cache(CACHE).put(key2, key2);

                tx.commit();
            }
        });
    }

    /**
     * Near = client
     * Primary = [0, 1, 2]
     */
    public void caseClientAllNodes() throws Exception {
        runCase(() -> {
            try (Transaction tx = clientNode().transactions().txStart()) {
                txOrigNode.put(tx.xid(), nodes());

                int key1 = key(CACHE, grid(0).localNode(), null);
                int key2 = key(CACHE, grid(1).localNode(), null);
                int key3 = key(CACHE, grid(2).localNode(), null);

                clientNode().cache(CACHE).put(key1, key1);
                clientNode().cache(CACHE).put(key2, key2);
                clientNode().cache(CACHE).put(key3, key3);

                tx.commit();
            }
        });
    }

    /**
     * Near = 0
     * Primary = [0]
     */
    public void caseCoordinatorOnly() throws Exception {
        runCase(() -> {
            try (Transaction tx = grid(0).transactions().txStart()) {
                txOrigNode.put(tx.xid(), 0);

                int key1 = key(CACHE, grid(0).localNode(), null);

                grid(0).cache(CACHE).put(key1, key1);

                tx.commit();
            }
        });
    }

    /**
     * Near = 0
     * Primary = [1]
     */
    public void caseCoordinatorOtherNode() throws Exception {
        runCase(() -> {
            try (Transaction tx = grid(0).transactions().txStart()) {
                txOrigNode.put(tx.xid(), 0);

                int key1 = key(CACHE, grid(1).localNode(), null);

                grid(0).cache(CACHE).put(key1, key1);

                tx.commit();
            }
        });
    }

    /**
     * Near = 0
     * Primary = [0, 1, 2]
     */
    public void caseCoordinatorAllNodes() throws Exception {
        runCase(() -> {
            try (Transaction tx = grid(0).transactions().txStart()) {
                txOrigNode.put(tx.xid(), 0);

                int key1 = key(CACHE, grid(0).localNode(), null);
                int key2 = key(CACHE, grid(1).localNode(), null);
                int key3 = key(CACHE, grid(2).localNode(), null);

                grid(0).cache(CACHE).put(key1, key1);
                grid(0).cache(CACHE).put(key2, key2);
                grid(0).cache(CACHE).put(key3, key3);

                tx.commit();
            }
        });
    }

    /**
     * Near = 1
     * Primary = [1]
     */
    public void caseNonCoordinatorOnly() throws Exception {
        runCase(() -> {
            try (Transaction tx = grid(1).transactions().txStart()) {
                txOrigNode.put(tx.xid(), 1);

                int key1 = key(CACHE, grid(1).localNode(), null);

                grid(1).cache(CACHE).put(key1, key1);

                tx.commit();
            }
        });
    }

    /**
     * Near = 1
     * Primary = [2]
     */
    public void caseNonCoordinatorOtherNode() throws Exception {
        runCase(() -> {
            try (Transaction tx = grid(1).transactions().txStart()) {
                txOrigNode.put(tx.xid(), 1);

                int key1 = key(CACHE, grid(2).localNode(), null);

                grid(1).cache(CACHE).put(key1, key1);

                tx.commit();
            }
        });
    }

    /**
     * Near = 1
     * Primary = [0, 1, 2]
     */
    public void caseNonCoordinatorAllNodes() throws Exception {
        runCase(() -> {
            try (Transaction tx = grid(1).transactions().txStart()) {
                txOrigNode.put(tx.xid(), 1);

                int key1 = key(CACHE, grid(0).localNode(), null);
                int key2 = key(CACHE, grid(1).localNode(), null);
                int key3 = key(CACHE, grid(2).localNode(), null);

                grid(1).cache(CACHE).put(key1, key1);
                grid(1).cache(CACHE).put(key2, key2);
                grid(1).cache(CACHE).put(key3, key3);

                tx.commit();
            }
        });
    }

    /** {@inheritDoc} */
    @Override protected int nodes() {
        return SRV_NODES;
    }

    /** {@inheritDoc} */
    @Override protected int backups() {
        return 0;
    }
}
