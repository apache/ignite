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

import org.apache.ignite.raft.jraft.FSMCaller;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.conf.ConfigurationManager;
import org.apache.ignite.raft.jraft.core.NodeMetrics;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryCodecFactory;
import org.apache.ignite.raft.jraft.entity.codec.v1.LogEntryV1CodecFactory;
import org.apache.ignite.raft.jraft.storage.LogStorage;

/**
 * Options for log manager.
 */
public class LogManagerOptions {
    private Node node;
    private LogStorage logStorage;
    private ConfigurationManager configurationManager;
    private FSMCaller fsmCaller;
    private int disruptorBufferSize = 1024;
    private RaftOptions raftOptions;
    private NodeMetrics nodeMetrics;
    private LogEntryCodecFactory logEntryCodecFactory = LogEntryV1CodecFactory.getInstance();

    public LogEntryCodecFactory getLogEntryCodecFactory() {
        return this.logEntryCodecFactory;
    }

    public void setLogEntryCodecFactory(final LogEntryCodecFactory logEntryCodecFactory) {
        this.logEntryCodecFactory = logEntryCodecFactory;
    }

    public NodeMetrics getNodeMetrics() {
        return this.nodeMetrics;
    }

    public void setNodeMetrics(final NodeMetrics nodeMetrics) {
        this.nodeMetrics = nodeMetrics;
    }

    public RaftOptions getRaftOptions() {
        return this.raftOptions;
    }

    public void setRaftOptions(final RaftOptions raftOptions) {
        this.raftOptions = raftOptions;
    }

    public int getDisruptorBufferSize() {
        return this.disruptorBufferSize;
    }

    public void setDisruptorBufferSize(final int disruptorBufferSize) {
        this.disruptorBufferSize = disruptorBufferSize;
    }

    public LogStorage getLogStorage() {
        return this.logStorage;
    }

    public void setLogStorage(final LogStorage logStorage) {
        this.logStorage = logStorage;
    }

    public ConfigurationManager getConfigurationManager() {
        return this.configurationManager;
    }

    public void setConfigurationManager(final ConfigurationManager configurationManager) {
        this.configurationManager = configurationManager;
    }

    public FSMCaller getFsmCaller() {
        return this.fsmCaller;
    }

    public void setFsmCaller(final FSMCaller fsmCaller) {
        this.fsmCaller = fsmCaller;
    }

    public void setNode(Node node) {
        this.node = node;
    }

    public Node getNode() {
        return node;
    }
}
