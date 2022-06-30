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

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteUuid;

/** Extended version of {@link ConsistentCutWalReader}, provides extended logging. */
public class ConsistentCutWalReaderEx extends ConsistentCutWalReader {
    /** Collection of transactions was run within a test. */
    private final Map<IgniteUuid, Integer> txNearNode;

    /** Ignite nodes description: (nodeIdx -> (consistentId, compactId)). */
    private final Map<Integer, T2<Serializable, Short>> txCompactNode;

    /** Order ID of Ignite node. */
    private final Integer nodeIdx;

    /** */
    ConsistentCutWalReaderEx(
        int nodeIdx,
        WALIterator walIter,
        IgniteLogger log,
        Map<IgniteUuid, Integer> txNearNode,
        Map<Integer, T2<Serializable, Short>> txCompactNode
    ) {
        super(walIter, txCompactNode.get(nodeIdx).get2(), log);

        this.nodeIdx = nodeIdx;
        this.txNearNode = txNearNode;
        this.txCompactNode = txCompactNode;
    }

    /** */
    @Override protected void logCommittedTxRecord(TxRecord tx, NodeConsistentCutState cut) {
        IgniteUuid uid = tx.nearXidVersion().asIgniteUuid();

        log("TX[id=" + uid + ", state=" + tx.state() + ", cut=" + cut.ver
            + ", origNodeId=" + txNearNode.get(uid) + ", nodeId=" + nodeIdx + ", participations="
            + cut.txParticipations.get(uid) + "]");
    }

    /** */
    @Override protected void logTxParticipations(TxRecord tx) {
        IgniteUuid uid = tx.nearXidVersion().asIgniteUuid();

        Map<Short, Collection<Short>> nodes = tx.participatingNodes();

        T2<Serializable, Short> nearNodeInfo = txCompactNode.get(txNearNode.get(uid));

        // -1 means client node.
        short origCompactId = -1;

        if (nearNodeInfo != null)
            origCompactId = nearNodeInfo.get2();

        log("PART[txId=" + uid + ", partNodes=" + nodes + ", origNodeCompactId=" + origCompactId + "]");
    }

    /** */
    @Override protected void logPreparedTxRecord(TxRecord tx, NodeConsistentCutState cut) {
        IgniteUuid uid = tx.nearXidVersion().asIgniteUuid();

        int nearNodeId = txNearNode.get(uid);

        short origCompactId = -1;

        if (txCompactNode.containsKey(nearNodeId))
            origCompactId = txCompactNode.get(nearNodeId).get2();

        log("TX[id=" + uid + ", state=" + tx.state() + ", cut=" + cut.ver +
            ", origNodeId=" + txNearNode.get(uid) + ", origCompactId=" + origCompactId +
            ", nodeId=" + nodeIdx + ", compactId=" + nodeCompactId +
            ", participations=" + cut.txParticipations.get(uid) + "]");
    }
}
