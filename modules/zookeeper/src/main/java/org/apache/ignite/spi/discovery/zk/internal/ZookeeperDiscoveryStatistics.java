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

import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Zookeeper discovery statistics.
 */
public class ZookeeperDiscoveryStatistics {
    /** */
    private final LongAdder joinedNodesCnt = new LongAdder();

    /** */
    private final LongAdder failedNodesCnt = new LongAdder();

    /** */
    private final LongAdder leftNodesCnt = new LongAdder();

    /** Communication error count. */
    private final LongAdder commErrCnt = new LongAdder();

    /** */
    public long joinedNodesCnt() {
        return joinedNodesCnt.longValue();
    }

    /** */
    public long failedNodesCnt() {
        return failedNodesCnt.longValue();
    }

    /** */
    public long leftNodesCnt() {
        return leftNodesCnt.longValue();
    }

    /** */
    public long commErrorCount() {
        return commErrCnt.longValue();
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ZookeeperDiscoveryStatistics.class, this);
    }
}
