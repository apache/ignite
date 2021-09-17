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

package org.apache.ignite.raft.jraft.rpc.impl.cli;

import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RpcContext;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;

/**
 * Ignite wrapper for {@link RpcRequestClosure}.
 * 
 * The main purpose: provide current leader id in error response
 * if any {@link RaftError#EPERM} issues with the current node.
 */
public class IgniteCliRpcRequestClosure implements Closure {
    /** Node, which call this closure. */
    private final Node node;
    
    /** Original closure. */
    private final RpcRequestClosure delegate;

    /** Creates new closure. */
    IgniteCliRpcRequestClosure(Node node, RpcRequestClosure closure) {
        this.node = node;
        this.delegate = closure;
    }

    /**
     * @see RpcRequestClosure#getRpcCtx() 
     */
    public RpcContext getRpcCtx() {
        return delegate.getRpcCtx();
    }

    /**
     * @see RpcRequestClosure#getMsgFactory()
     */
    public RaftMessagesFactory getMsgFactory() {
        return delegate.getMsgFactory();
    }
    
    /**
     * @see RpcRequestClosure#sendResponse(Message)
     */
    public void sendResponse(Message msg) {
        if (msg instanceof RpcRequests.ErrorResponse) {
            RpcRequests.ErrorResponse err = (RpcRequests.ErrorResponse) msg;

            PeerId newLeader;

            if (err.errorCode() == RaftError.EPERM.getNumber()) {
                newLeader = node.getLeaderId();

                delegate.sendResponse(
                    RaftRpcFactory.DEFAULT
                        .newResponse(newLeader.toString(), getMsgFactory(), RaftError.EPERM, err.errorMsg()));
                return;
            }
        }

        delegate.sendResponse(msg);
    }
    
    /**
     * @see RpcRequestClosure#run(Status) 
     */
    @Override public void run(Status status) {
        sendResponse(RaftRpcFactory.DEFAULT.newResponse(getMsgFactory(), status));
    }
}
