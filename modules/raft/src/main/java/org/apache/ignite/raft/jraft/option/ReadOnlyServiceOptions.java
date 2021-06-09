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

/**
 * Read-Only service options.
 */
public class ReadOnlyServiceOptions {

    private RaftOptions raftOptions;
    private NodeImpl node;
    private FSMCaller fsmCaller;

    public NodeImpl getNode() {
        return node;
    }

    public void setNode(NodeImpl node) {
        this.node = node;
    }

    public RaftOptions getRaftOptions() {
        return raftOptions;
    }

    public void setRaftOptions(RaftOptions raftOptions) {
        this.raftOptions = raftOptions;
    }

    public FSMCaller getFsmCaller() {
        return fsmCaller;
    }

    public void setFsmCaller(FSMCaller fsm) {
        this.fsmCaller = fsm;
    }
}
