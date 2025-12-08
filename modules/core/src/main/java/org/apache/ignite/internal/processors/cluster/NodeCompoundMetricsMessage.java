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

package org.apache.ignite.internal.processors.cluster;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/** Node compound metrics message. */
public final class NodeCompoundMetricsMessage implements Message {
    /** */
    public static final short TYPE_CODE = 138;

    /** Node metrics wrapper message. */
    @Order(0)
    private NodeMetricsMessage nodeMetricsMsg;

    /** Cache metrics wrapper message. */
    @Order(1)
    private Map<Integer, CacheMetricsMessage> cachesMetrics;

    /** Empty constructor for {@link GridIoMessageFactory}. */
    public NodeCompoundMetricsMessage() {

    }

    /** */
    public NodeCompoundMetricsMessage(ClusterMetrics nodeMetrics, Map<Integer, CacheMetrics> cacheMetrics) {
        nodeMetricsMsg = new NodeMetricsMessage(nodeMetrics);

        cachesMetrics = new HashMap<>(cacheMetrics.size(), 1.0f);

        cacheMetrics.forEach((key, value) -> cachesMetrics.put(key, new CacheMetricsMessage(value)));
    }

    /** */
    public Map<Integer, CacheMetricsMessage> cachesMetrics() {
        return cachesMetrics;
    }

    /** */
    public void cachesMetrics(Map<Integer, CacheMetricsMessage> cacheMetricsMsg) {
        cachesMetrics = cacheMetricsMsg;
    }

    /** */
    public NodeMetricsMessage nodeMetricsMsg() {
        return nodeMetricsMsg;
    }

    /** */
    public void nodeMetricsMsg(NodeMetricsMessage nodeMetricsMsg) {
        this.nodeMetricsMsg = nodeMetricsMsg;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(NodeCompoundMetricsMessage.class, this);
    }
}
