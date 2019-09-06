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

package org.apache.ignite.spi.metric.list.view;

import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.metric.list.walker.Order;
import org.apache.ignite.spi.metric.list.MonitoringList;
import org.apache.ignite.spi.metric.list.MonitoringRow;

import static org.apache.ignite.internal.util.IgniteUtils.toStringSafe;

/**
 * Cluster node representation for a {@link MonitoringList}.
 */
public class ClusterNodeView implements MonitoringRow<UUID> {
    /** Cluster node. */
    private final ClusterNode n;

    /**
     * @param n Cluster node.
     */
    public ClusterNodeView(ClusterNode n) {
        this.n = n;
    }

    /** {@inheritDoc} */
    @Override public UUID monitoringRowId() {
        return id();
    }

    /**
     * @return Node id.
     * @see ClusterNode#id()
     */
    @Order
    public UUID id() {
        return n.id();
    }

    /**
     * @return Node consistend id.
     * @see ClusterNode#consistentId()
     */
    @Order(1)
    public String consistentId() {
        return toStringSafe(n.consistentId());
    }

    /**
     * @return Addresses.
     * @see ClusterNode#addresses()
     * */
    @Order(2)
    public String addresses() {
        return toStringSafe(n.addresses());
    }

    /**
     * @return Addresses string.
     * @see ClusterNode#hostNames()
     */
    @Order(3)
    public String hostNames() {
        return toStringSafe(n.hostNames());
    }

    /**
     * @return Topology order.
     * @see ClusterNode#order()
     */
    @Order(4)
    public long order() {
        return n.order();
    }

    /**
     * @return Version.
     * @see ClusterNode#version()
     */
    public String version() {
        return n.version().toString();
    }

    /**
     * @return {@code True} if node local.
     * @see ClusterNode#isLocal()
     */
    public boolean isLocal() {
        return n.isLocal();
    }

    /**
     * @return {@code True} if node is daemon.
     * @see ClusterNode#isDaemon()
     */
    public boolean isDaemon() {
        return n.isDaemon();
    }

    /**
     * @return {@code True} if node is client.
     * @see ClusterNode#isClient() ()
     */
    public boolean isClient() {
        return n.isClient();
    }
}
