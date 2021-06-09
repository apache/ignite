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
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftServerService;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.RequestVoteRequest;

/**
 * Handle PreVote and RequestVote requests.
 */
public class RequestVoteRequestProcessor extends NodeRequestProcessor<RequestVoteRequest> {

    public RequestVoteRequestProcessor(Executor executor) {
        super(executor, RpcRequests.RequestVoteResponse.getDefaultInstance());
    }

    @Override
    protected String getPeerId(final RequestVoteRequest request) {
        return request.getPeerId();
    }

    @Override
    protected String getGroupId(final RequestVoteRequest request) {
        return request.getGroupId();
    }

    @Override
    public Message processRequest0(final RaftServerService service, final RequestVoteRequest request,
        final RpcRequestClosure done) {
        if (request.getPreVote()) {
            return service.handlePreVoteRequest(request);
        }
        else {
            return service.handleRequestVoteRequest(request);
        }
    }

    @Override
    public String interest() {
        return RequestVoteRequest.class.getName();
    }
}
