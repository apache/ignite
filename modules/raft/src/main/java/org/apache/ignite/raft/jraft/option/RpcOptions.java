/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.option;

import java.util.concurrent.ExecutorService;
import com.codahale.metrics.MetricRegistry;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.rpc.RpcClient;

public class RpcOptions {
    /** Raft message factory. */
    private RaftMessagesFactory raftMessagesFactory = new RaftMessagesFactory();

    /**
     * Rpc connect timeout in milliseconds.
     * Default: 1200 (1.2s)
     */
    private int rpcConnectTimeoutMs = 1200;

    /**
     * RPC request default timeout in milliseconds.
     * Default: 5000 (5s)
     */
    private int rpcDefaultTimeout = 5000;

    /**
     * Install snapshot RPC request default timeout in milliseconds.
     * Default: 5m
     */
    private int rpcInstallSnapshotTimeout = 5 * 60 * 1000;

    /**
     * RPC process thread pool size.
     * Default: 80
     */
    private int rpcProcessorThreadPoolSize = 80;

    /**
     * Whether to enable checksum for RPC.
     */
    private boolean enableRpcChecksum = false;

    /**
     * Client instance.
     */
    private RpcClient rpcClient;

    /**
     * The client executor is used by RPC client.
     */
    private ExecutorService clientExecutor;

    /**
     * Metric registry for RPC services, user should not use this field.
     */
    private MetricRegistry metricRegistry;

    public int getRpcConnectTimeoutMs() {
        return this.rpcConnectTimeoutMs;
    }

    public void setRpcConnectTimeoutMs(int rpcConnectTimeoutMs) {
        this.rpcConnectTimeoutMs = rpcConnectTimeoutMs;
    }

    public int getRpcDefaultTimeout() {
        return this.rpcDefaultTimeout;
    }

    public void setRpcDefaultTimeout(int rpcDefaultTimeout) {
        this.rpcDefaultTimeout = rpcDefaultTimeout;
    }

    public int getRpcInstallSnapshotTimeout() {
        return rpcInstallSnapshotTimeout;
    }

    public void setRpcInstallSnapshotTimeout(int rpcInstallSnapshotTimeout) {
        this.rpcInstallSnapshotTimeout = rpcInstallSnapshotTimeout;
    }

    public int getRpcProcessorThreadPoolSize() {
        return this.rpcProcessorThreadPoolSize;
    }

    public void setRpcProcessorThreadPoolSize(int rpcProcessorThreadPoolSize) {
        this.rpcProcessorThreadPoolSize = rpcProcessorThreadPoolSize;
    }

    public boolean isEnableRpcChecksum() {
        return enableRpcChecksum;
    }

    public void setEnableRpcChecksum(boolean enableRpcChecksum) {
        this.enableRpcChecksum = enableRpcChecksum;
    }

    public MetricRegistry getMetricRegistry() {
        return metricRegistry;
    }

    public void setMetricRegistry(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    public RpcClient getRpcClient() {
        return rpcClient;
    }

    public void setRpcClient(RpcClient rpcClient) {
        this.rpcClient = rpcClient;
    }

    public ExecutorService getClientExecutor() {
        return clientExecutor;
    }

    public void setClientExecutor(ExecutorService clientExecutor) {
        this.clientExecutor = clientExecutor;
    }

    /**
     * @return Raft message factory.
     */
    public RaftMessagesFactory getRaftMessagesFactory() {
        return raftMessagesFactory;
    }

    /**
     * Sets the Raft message factory.
     */
    public void setRaftMessagesFactory(RaftMessagesFactory raftMessagesFactory) {
        this.raftMessagesFactory = raftMessagesFactory;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return "RpcOptions{" + "rpcConnectTimeoutMs=" + rpcConnectTimeoutMs + ", rpcDefaultTimeout="
            + rpcDefaultTimeout + ", rpcInstallSnapshotTimeout=" + rpcInstallSnapshotTimeout
            + ", rpcProcessorThreadPoolSize=" + rpcProcessorThreadPoolSize + ", enableRpcChecksum="
            + enableRpcChecksum + ", metricRegistry=" + metricRegistry + '}';
    }
}
