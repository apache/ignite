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
package org.apache.ignite.raft.jraft;

import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RpcOptions;
import org.apache.ignite.raft.jraft.rpc.RpcServer;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.apache.ignite.raft.jraft.util.StringUtils;
import org.apache.ignite.raft.jraft.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A raft group service.
 */
public class RaftGroupService {
    private static final Logger LOG = LoggerFactory.getLogger(RaftGroupService.class);

    private volatile boolean started = false;

    /**
     * This node serverId
     */
    private PeerId serverId;

    /**
     * Node options
     */
    private NodeOptions nodeOptions;

    /**
     * The raft RPC server
     */
    private RpcServer rpcServer;

    /**
     * If we want to share the rpcServer instance, then we can't stop it when shutdown.
     */
    private final boolean sharedRpcServer;

    /**
     * The raft group id
     */
    private String groupId;

    /**
     * The raft node.
     */
    private Node node;

    /**
     * The node manager.
     */
    private NodeManager nodeManager;

    /**
     * @param groupId Group Id.
     * @param serverId Server id.
     * @param nodeOptions Node options.
     * @param rpcServer RPC server.
     * @param nodeManager Node manager.
     */
    public RaftGroupService(final String groupId, final PeerId serverId, final NodeOptions nodeOptions,
        final RpcServer rpcServer, final NodeManager nodeManager) {
        this(groupId, serverId, nodeOptions, rpcServer, nodeManager, false);
    }

    /**
     * @param groupId Group Id.
     * @param serverId Server id.
     * @param nodeOptions Node options.
     * @param rpcServer RPC server.
     * @param nodeManager Node manager.
     * @param sharedRpcServer {@code True} if a shared server.
     */
    public RaftGroupService(final String groupId, final PeerId serverId, final NodeOptions nodeOptions,
        final RpcServer rpcServer, final NodeManager nodeManager, final boolean sharedRpcServer) {
        super();
        this.groupId = groupId;
        this.serverId = serverId;
        this.nodeOptions = nodeOptions;
        this.rpcServer = rpcServer;
        this.nodeManager = nodeManager;
        this.sharedRpcServer = sharedRpcServer;
    }

    public synchronized Node getRaftNode() {
        return this.node;
    }

    /**
     * Starts the raft group service, returns the raft node.
     */
    public synchronized Node start() {
        if (this.started) {
            return this.node;
        }
        if (this.serverId == null || this.serverId.getEndpoint() == null
            || this.serverId.getEndpoint().equals(new Endpoint(Utils.IP_ANY, 0))) {
            throw new IllegalArgumentException("Blank serverId:" + this.serverId);
        }
        if (StringUtils.isBlank(this.groupId)) {
            throw new IllegalArgumentException("Blank group id" + this.groupId);
        }

        assert this.nodeOptions.getRpcClient() != null;

        // Should start RPC server before node initialization to avoid race.
        if (!sharedRpcServer) {
            this.rpcServer.init(null);
        }
        else {
            LOG.info("RPC server is shared by RaftGroupService.");
        }

        this.node = new NodeImpl(groupId, serverId);

        if (!this.node.init(this.nodeOptions)) {
            LOG.warn("Stopping partially started node [groupId={}, serverId={}]", groupId, serverId);
            this.node.shutdown();

            try {
                this.node.join();
            }
            catch (InterruptedException e) {
                throw new IgniteInternalException(e);
            }

            throw new IgniteInternalException("Fail to init node, please see the logs to find the reason.");
        }

        this.nodeManager.add(this.node);
        this.started = true;
        LOG.info("Start the RaftGroupService successfully {}", this.node.getNodeId());
        return this.node;
    }

    public synchronized void shutdown() {
        // TODO asch remove handlers before shutting down raft node https://issues.apache.org/jira/browse/IGNITE-14519
        if (this.rpcServer != null && !this.sharedRpcServer) {
            try {
                this.rpcServer.shutdown();
            }
            catch (Exception e) {
                LOG.error("Failed to shutdown the server", e);
            }
            this.rpcServer = null;
        }

        if (!this.started) {
            return;
        }

        this.node.shutdown();
        try {
            this.node.join();
        }
        catch (InterruptedException e) {
            LOG.error("Interrupted while waiting for the node to shutdown");
        }

        nodeManager.remove(this.node);
        this.started = false;
        LOG.info("Stop the RaftGroupService successfully.");
    }

    /**
     * Returns true when service is started.
     */
    public boolean isStarted() {
        return this.started;
    }

    /**
     * Returns the raft group id.
     */
    public String getGroupId() {
        return this.groupId;
    }

    /**
     * Set the raft group id
     */
    public void setGroupId(final String groupId) {
        if (this.started) {
            throw new IllegalStateException("Raft group service already started");
        }
        this.groupId = groupId;
    }

    /**
     * Returns the node serverId
     */
    public PeerId getServerId() {
        return this.serverId;
    }

    /**
     * Set the node serverId
     */
    public void setServerId(final PeerId serverId) {
        if (this.started) {
            throw new IllegalStateException("Raft group service already started");
        }
        this.serverId = serverId;
    }

    /**
     * Returns the node options.
     */
    public RpcOptions getNodeOptions() {
        return this.nodeOptions;
    }

    /**
     * Set node options.
     */
    public void setNodeOptions(final NodeOptions nodeOptions) {
        if (this.started) {
            throw new IllegalStateException("Raft group service already started");
        }
        if (nodeOptions == null) {
            throw new IllegalArgumentException("Invalid node options.");
        }
        nodeOptions.validate();
        this.nodeOptions = nodeOptions;
    }

    /**
     * Returns the rpc server instance.
     */
    public RpcServer getRpcServer() {
        return this.rpcServer;
    }

    /**
     * Set rpc server.
     */
    public void setRpcServer(final RpcServer rpcServer) {
        if (this.started) {
            throw new IllegalStateException("Raft group service already started");
        }
        if (this.serverId == null) {
            throw new IllegalStateException("Please set serverId at first");
        }
        if (rpcServer.boundPort() != this.serverId.getPort()) {
            throw new IllegalArgumentException("RPC server port mismatch");
        }
        this.rpcServer = rpcServer;
    }
}
