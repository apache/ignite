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

package org.apache.ignite.spi.discovery.tcp.messages;

import java.util.Collection;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.plugin.extensions.communication.Message;

/** A container message for a collection of {@link ClusterNodeMessage}. */
public class ClusterNodeCollectionMessage implements Message {
    /** The collection of wrapped {@link ClusterNodeMessage}. */
    @Order(value = 0, method = "clusterNodeMessages")
    private Collection<ClusterNodeMessage> clusterNodeMsgs;

    /** Constructor for {@link DiscoveryMessageFactory}. */
    public ClusterNodeCollectionMessage() {
        // No-op.
    }

    /** @param clusterNodeMsgs Holder messages of {@link ClusterNode}. */
    public ClusterNodeCollectionMessage(Collection<ClusterNodeMessage> clusterNodeMsgs) {
        this.clusterNodeMsgs = clusterNodeMsgs;
    }

    /** @return Holder messages of {@link ClusterNode}. */
    public Collection<ClusterNodeMessage> clusterNodeMessages() {
        return clusterNodeMsgs;
    }

    /** @param clusterNodeMsgs Holder messages of {@link ClusterNode}. */
    public void clusterNodeMessages(Collection<ClusterNodeMessage> clusterNodeMsgs) {
        this.clusterNodeMsgs = clusterNodeMsgs;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -110;
    }
}
