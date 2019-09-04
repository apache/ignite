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

import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.metric.list.walker.Order;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.lang.GridFunc;
import org.apache.ignite.spi.metric.list.MonitoringList;
import org.apache.ignite.spi.metric.list.MonitoringRow;
import org.jetbrains.annotations.Nullable;

/**
 * Cluster node representation for a {@link MonitoringList}.
 */
public class ClusterNodeView implements MonitoringRow<UUID> {
    /** */
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

    /** */
    @Order
    public UUID id() {
        return n.id();
    }

    /** */
    @Order(1)
    public String consistentId() {
        return IgniteUtils.toStringSafe(n.consistentId());
    }

    /** */
    @Order(2)
    public String addresses() {
        return toStringSafe(n.addresses());
    }

    /** */
    @Order(3)
    public String hostNames() {
        return toStringSafe(n.hostNames());
    }

    /** */
    @Order(4)
    public long order() {
        return n.order();
    }

    /** */
    public String version() {
        return n.version().toString();
    }

    /** */
    public boolean isLocal() {
        return n.isLocal();
    }

    /** */
    public boolean isDaemon() {
        return n.isDaemon();
    }

    /** */
    public boolean isClient() {
        return n.isClient();
    }

    /**
     * @param objs Collection of objects.
     * @return String representation
     */
    @Nullable public static String toStringSafe(@Nullable Collection<String> objs) {
        if (GridFunc.isEmpty(objs))
            return null;
        else {
            StringBuilder str = new StringBuilder();

            Iterator<String> iObj = objs.iterator();

            str.append(iObj.next());

            while(iObj.hasNext())
                str.append(',').append(' ').append(iObj.next());

            return str.toString();
        }
    }
}
