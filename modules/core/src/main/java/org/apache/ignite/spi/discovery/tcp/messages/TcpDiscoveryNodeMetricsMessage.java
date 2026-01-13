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

import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.internal.managers.discovery.DiscoveryMessageFactory;
import org.apache.ignite.internal.processors.cluster.NodeMetricsMessage;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * We cannot directly reuse `NodeMetricsMessage` in Discovery as it is registered in a message factory of Communication
 * component and thus is unavailable in Discovery. We have to extend `NodeMetricsMessage` and register this subclass in
 * message factory of Discovery component.
 */
public class TcpDiscoveryNodeMetricsMessage extends NodeMetricsMessage {
    /** Constructor for {@link DiscoveryMessageFactory}. */
    public TcpDiscoveryNodeMetricsMessage() {
        // No-op.
    }

    /** @param metrics Metrics. */
    public TcpDiscoveryNodeMetricsMessage(ClusterMetrics metrics) {
        super(metrics);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -102;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TcpDiscoveryNodeMetricsMessage.class, this, "super", super.toString());
    }
}
