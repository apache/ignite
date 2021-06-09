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
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.storage.LogManager;
import org.apache.ignite.raft.jraft.storage.SnapshotThrottle;
import org.apache.ignite.raft.jraft.util.Endpoint;

/**
 * Snapshot executor options.
 */
public class SnapshotExecutorOptions {

    // URI of SnapshotStorage
    private String uri;
    private FSMCaller fsmCaller;
    private NodeImpl node;
    private LogManager logManager;
    private long initTerm;
    private Endpoint addr;
    private boolean filterBeforeCopyRemote;
    private SnapshotThrottle snapshotThrottle;

    public SnapshotThrottle getSnapshotThrottle() {
        return snapshotThrottle;
    }

    public void setSnapshotThrottle(SnapshotThrottle snapshotThrottle) {
        this.snapshotThrottle = snapshotThrottle;
    }

    public String getUri() {
        return this.uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public FSMCaller getFsmCaller() {
        return this.fsmCaller;
    }

    public void setFsmCaller(FSMCaller fsmCaller) {
        this.fsmCaller = fsmCaller;
    }

    public NodeImpl getNode() {
        return this.node;
    }

    public void setNode(NodeImpl node) {
        this.node = node;
    }

    public LogManager getLogManager() {
        return this.logManager;
    }

    public void setLogManager(LogManager logManager) {
        this.logManager = logManager;
    }

    public long getInitTerm() {
        return this.initTerm;
    }

    public void setInitTerm(long initTerm) {
        this.initTerm = initTerm;
    }

    public Endpoint getAddr() {
        return this.addr;
    }

    public void setAddr(Endpoint addr) {
        this.addr = addr;
    }

    public boolean isFilterBeforeCopyRemote() {
        return this.filterBeforeCopyRemote;
    }

    public void setFilterBeforeCopyRemote(boolean filterBeforeCopyRemote) {
        this.filterBeforeCopyRemote = filterBeforeCopyRemote;
    }
}
