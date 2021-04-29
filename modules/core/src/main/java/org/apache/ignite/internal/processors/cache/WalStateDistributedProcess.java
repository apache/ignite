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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;

/**
 * Distributed process governing WAL state change.
 */
public class WalStateDistributedProcess {
    /** Original propose message. */
    private final WalStateProposeMessage msg;

    /** Remaining nodes. */
    @GridToStringInclude
    private final Collection<UUID> remainingNodes;

    /** Acks. */
    private final Map<UUID, WalStateAckMessage> acks;

    /**
     * Constructor.
     *
     * @param msg Original propose message.
     * @param remainingNodes Remaining nodes.
     */
    public WalStateDistributedProcess(WalStateProposeMessage msg, Collection<UUID> remainingNodes) {
        assert !F.isEmpty(remainingNodes);

        this.msg = msg;
        this.remainingNodes = remainingNodes;

        acks = new HashMap<>(remainingNodes.size());
    }

    /**
     * Handle node finish.
     *
     * @param nodeId Node ID.
     * @param ack Ack message.
     */
    public void onNodeFinished(UUID nodeId, WalStateAckMessage ack) {
        remainingNodes.remove(nodeId);

        // Log only messages from affinity nodes. Non-affinity nodes are of no interest for us since they do not have
        // group context in general case and hence we cannot tell fo sure whether anything was changed or not.
        if (ack.affNode())
            acks.put(nodeId, ack);
    }

    /**
     * Handle node leave.
     *
     * @param nodeId Node ID.
     */
    public void onNodeLeft(UUID nodeId) {
        remainingNodes.remove(nodeId);
    }

    /**
     * @return {@code True} if process is completed.
     */
    public boolean completed() {
        return remainingNodes.isEmpty();
    }

    /**
     * @return Operation ID.
     */
    public UUID operationId() {
        return msg.operationId();
    }

    /**
     * Prepare finish message based on obtained results.
     *
     * @return Message.
     */
    public WalStateFinishMessage prepareFinishMessage() {
        assert completed();

        if (acks.isEmpty()) {
            // We haven't received any response from affinity nodes. Result is unknown, so throw an exception.
            return new WalStateFinishMessage(msg.operationId(), msg.groupId(), msg.groupDeploymentId(), false,
                "Operation result is unknown because all affinity nodes have left the grid.");
        }

        Map<UUID, String> errs = new HashMap<>();

        // Look for errors first.
        for (Map.Entry<UUID, WalStateAckMessage> ackEntry : acks.entrySet()) {
            UUID nodeId = ackEntry.getKey();
            WalStateAckMessage ackMsg = ackEntry.getValue();

            assert ackMsg.affNode();

            if (ackMsg.errorMessage() != null)
                errs.put(nodeId, ackMsg.errorMessage());
        }

        if (!errs.isEmpty()) {
            SB errMsg =
                new SB("Operation failed on some nodes (please consult to node logs for more information) [");

            boolean first = true;

            for (Map.Entry<UUID, String> err : errs.entrySet()) {
                if (first)
                    first = false;
                else
                    errMsg.a(", ");

                errMsg.a("[nodeId=" + err.getKey() + ", err=" + err.getValue() + ']');
            }

            errMsg.a(']');

            return new WalStateFinishMessage(msg.operationId(), msg.groupId(), msg.groupDeploymentId(), false,
                errMsg.toString());
        }

        // Verify results consistency. Note that non-affinity node are not present at this stage.
        Boolean changed = null;

        for (WalStateAckMessage ackMsg : acks.values()) {
            boolean curChanged = ackMsg.changed();

            if (changed == null)
                changed = curChanged;
            else {
                if (!F.eq(curChanged, changed)) {
                    return new WalStateFinishMessage(msg.operationId(), msg.groupId(), msg.groupDeploymentId(),
                        false, "Operation result is unknown because nodes reported different results (please " +
                        "re-try operation).");
                }
            }
        }

        // All nodes completed operation with the same result, complete with success.
        assert changed != null;

        return new WalStateFinishMessage(msg.operationId(), msg.groupId(), msg.groupDeploymentId(), changed, null);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WalStateDistributedProcess.class, this);
    }
}
