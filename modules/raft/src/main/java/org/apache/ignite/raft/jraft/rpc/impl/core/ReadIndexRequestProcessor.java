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
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftServerService;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.ReadIndexRequest;
import org.apache.ignite.raft.jraft.rpc.RpcResponseClosureAdapter;

/**
 * Handle read index request.
 */
public class ReadIndexRequestProcessor extends NodeRequestProcessor<ReadIndexRequest> {
    public ReadIndexRequestProcessor(Executor executor, RaftMessagesFactory msgFactory) {
        super(executor, msgFactory);
    }

    @Override
    protected String getPeerId(final ReadIndexRequest request) {
        return request.peerId();
    }

    @Override
    protected String getGroupId(final ReadIndexRequest request) {
        return request.groupId();
    }

    @Override
    public Message processRequest0(final RaftServerService service, final ReadIndexRequest request,
        final RpcRequestClosure done) {
        service.handleReadIndexRequest(request, new RpcResponseClosureAdapter<RpcRequests.ReadIndexResponse>() {

            @Override
            public void run(final Status status) {
                if (getResponse() != null) {
                    done.sendResponse(getResponse());
                }
                else {
                    done.run(status);
                }
            }

        });
        return null;
    }

    @Override
    public String interest() {
        return ReadIndexRequest.class.getName();
    }
}
