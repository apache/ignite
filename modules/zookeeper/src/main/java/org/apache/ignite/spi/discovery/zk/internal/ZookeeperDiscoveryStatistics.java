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
