/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.spi.discovery.zk.internal;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Zookeeper discovery statistics.
 */
public class ZookeeperDiscoveryStatistics {
    /** */
    private long joinedNodesCnt;

    /** */
    private long failedNodesCnt;

    /** Communication error count. */
    private long commErrCnt;

    /** */
    public long joinedNodesCnt() {
        return joinedNodesCnt;
    }

    /** */
    public long failedNodesCnt() {
        return failedNodesCnt;
    }

    /** */
    public long commErrorCount() {
        return commErrCnt;
    }

    /** */
    public void onNodeJoined() {
        joinedNodesCnt++;
    }

    /** */
    public void onNodeFailed() {
        failedNodesCnt++;
    }

    /** */
    public void onCommunicationError() {
        commErrCnt++;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ZookeeperDiscoveryStatistics.class, this);
    }
}
