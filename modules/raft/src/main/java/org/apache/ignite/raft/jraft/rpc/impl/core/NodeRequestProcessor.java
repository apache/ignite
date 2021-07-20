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
package org.apache.ignite.raft.jraft.rpc.impl.core;

import java.util.concurrent.Executor;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RaftServerService;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;
import org.apache.ignite.raft.jraft.rpc.RpcRequestProcessor;

/**
 * Node handle requests processor template.
 *
 * @param <T> Message
 * @author jiachun.fjc
 */
public abstract class NodeRequestProcessor<T extends Message> extends RpcRequestProcessor<T> {
    public NodeRequestProcessor(Executor executor, RaftMessagesFactory msgFactory) {
        super(executor, msgFactory);
    }

    protected abstract Message processRequest0(final RaftServerService serviceService, final T request,
        final RpcRequestClosure done);

    protected abstract String getPeerId(final T request);

    protected abstract String getGroupId(final T request);

    @Override
    public Message processRequest(final T request, final RpcRequestClosure done) {
        final PeerId peer = new PeerId();
        final String peerIdStr = getPeerId(request);
        if (peer.parse(peerIdStr)) {
            final String groupId = getGroupId(request);
            final Node node = done.getRpcCtx().getNodeManager().get(groupId, peer);
            if (node != null) {
                return processRequest0((RaftServerService) node, request, done);
            }
            else {
                return RaftRpcFactory.DEFAULT //
                    .newResponse(msgFactory(), RaftError.ENOENT, "Peer id not found: %s, group: %s", peerIdStr,
                        groupId);
            }
        }
        else {
            return RaftRpcFactory.DEFAULT //
                .newResponse(msgFactory(), RaftError.EINVAL, "Fail to parse peerId: %s", peerIdStr);
        }
    }
}
