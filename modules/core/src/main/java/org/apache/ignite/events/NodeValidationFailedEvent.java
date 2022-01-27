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

package org.apache.ignite.events;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.IgniteNodeValidationResult;

import static org.apache.ignite.events.EventType.EVT_NODE_VALIDATION_FAILED;

/**
 * This event is triggered if any of {@link GridComponent}s fail to validate the joining node
 * while join message processing.
 *
 * @see EventType#EVT_NODE_VALIDATION_FAILED
 * @see GridComponent#validateNode
 */
public class NodeValidationFailedEvent extends EventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** The node that attempted to join cluster. */
    private final ClusterNode evtNode;

    /** Validation result. */
    private final IgniteNodeValidationResult res;

    /**
     * Creates new node validation event with given parameters.
     *
     * @param node Local node.
     * @param evtNode Node which couldn't join the topology due to a validation failure.
     * @param res Joining node validation result.
     */
    public NodeValidationFailedEvent(ClusterNode node, ClusterNode evtNode, IgniteNodeValidationResult res) {
        super(node, res.message(), EVT_NODE_VALIDATION_FAILED);

        this.evtNode = evtNode;
        this.res = res;
    }

    /** @return Node that couldn't join the topology due to a validation failure. */
    public ClusterNode eventNode() {
        return evtNode;
    }

    /** @return Joining node validation result. */
    public IgniteNodeValidationResult validationResult() {
        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(NodeValidationFailedEvent.class, this, "parent", super.toString());
    }
}
