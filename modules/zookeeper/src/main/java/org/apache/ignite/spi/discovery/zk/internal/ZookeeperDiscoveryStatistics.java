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
package org.apache.ignite.spi.discovery.zk.internal;

import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Zookeeper discovery statistics.
 */
public class ZookeeperDiscoveryStatistics {
    /** */
    private final LongAdderMetric joinedNodesCnt = new LongAdderMetric("JoinedNodes", "Joined nodes count");

    /** */
    private final LongAdderMetric failedNodesCnt = new LongAdderMetric("FailedNodes", "Failed nodes count");

    /** */
    private final LongAdderMetric leftNodesCnt = new LongAdderMetric("LeftNodes", "Left nodes count");

    /** Communication error count. */
    private final LongAdderMetric commErrCnt = new LongAdderMetric("CommunicationErrors", "Communication errors count");

    /** Current topology version */
    private final AtomicLongMetric topVer = new AtomicLongMetric("CurrentTopologyVersion", "Current topology version");

    /**
     * @param discoReg Discovery metric registry.
     */
    public void registerMetrics(MetricRegistry discoReg) {
        discoReg.register(joinedNodesCnt);
        discoReg.register(failedNodesCnt);
        discoReg.register(leftNodesCnt);
        discoReg.register(commErrCnt);
        discoReg.register(topVer);
    }

    /** */
    public long joinedNodesCnt() {
        return joinedNodesCnt.value();
    }

    /** */
    public long failedNodesCnt() {
        return failedNodesCnt.value();
    }

    /** */
    public long leftNodesCnt() {
        return leftNodesCnt.value();
    }

    /** */
    public long commErrorCount() {
        return commErrCnt.value();
    }

    /** */
    public void onNodeJoined() {
        joinedNodesCnt.increment();
    }

    /** */
    public void onNodeFailed() {
        failedNodesCnt.increment();
    }

    /** */
    public void onNodeLeft() {
        leftNodesCnt.increment();
    }

    /** */
    public void onCommunicationError() {
        commErrCnt.increment();
    }

    /** */
    public void onTopologyChanged(long topVer) {
        this.topVer.value(topVer);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ZookeeperDiscoveryStatistics.class, this);
    }
}
