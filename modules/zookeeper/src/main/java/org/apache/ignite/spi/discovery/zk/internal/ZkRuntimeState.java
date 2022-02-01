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

import java.util.List;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.Watcher;

/**
 *
 */
class ZkRuntimeState {
    /** */
    ZkWatcher watcher;

    /** */
    ZkAliveNodeDataWatcher aliveNodeDataWatcher;

    /** */
    volatile Exception errForClose;

    /** */
    final boolean reconnect;

    /** */
    volatile ZookeeperClient zkClient;

    /** */
    long internalOrder;

    /** */
    int joinDataPartCnt;

    /** */
    long gridStartTime;

    /** */
    volatile boolean joined;

    /** */
    ZkDiscoveryEventsData evtsData;

    /** */
    boolean crd;

    /** */
    String locNodeZkPath;

    /** */
    final ZkAliveNodeData locNodeInfo = new ZkAliveNodeData();

    /** */
    int procEvtCnt;

    /** */
    final ZkClusterNodes top = new ZkClusterNodes();

    /** */
    List<ClusterNode> commErrProcNodes;

    /** Timeout callback registering watcher for join error
     * (set this watcher after timeout as a minor optimization).
     */
    ZkTimeoutObject joinErrTo;

    /** Timeout callback set to wait for join timeout. */
    ZkTimeoutObject joinTo;

    /** Timeout callback to update processed events counter. */
    ZkTimeoutObject procEvtsUpdateTo;

    /** */
    boolean updateAlives;

    /**
     * @param reconnect {@code True} if joined topology before reconnect attempt.
     */
    ZkRuntimeState(boolean reconnect) {
        this.reconnect = reconnect;
    }

    /**
     * @param watcher Watcher.
     * @param aliveNodeDataWatcher Alive nodes data watcher.
     */
    void init(ZkWatcher watcher, ZkAliveNodeDataWatcher aliveNodeDataWatcher) {
        this.watcher = watcher;
        this.aliveNodeDataWatcher = aliveNodeDataWatcher;
    }

    /**
     * @param err Error.
     */
    void onCloseStart(Exception err) {
        assert err != null;

        errForClose = err;

        ZookeeperClient zkClient = this.zkClient;

        if (zkClient != null)
            zkClient.onCloseStart();
    }

    /**
     *
     */
    interface ZkWatcher extends Watcher, AsyncCallback.Children2Callback, AsyncCallback.DataCallback {
        // No-op.
    }

    /**
     *
     */
    interface ZkAliveNodeDataWatcher extends Watcher, AsyncCallback.DataCallback {
        // No-op.
    }
}
