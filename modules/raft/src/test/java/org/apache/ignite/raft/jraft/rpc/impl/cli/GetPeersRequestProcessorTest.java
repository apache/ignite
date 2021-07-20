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
package org.apache.ignite.raft.jraft.rpc.impl.cli;

import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.rpc.CliRequests.GetPeersRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.GetPeersResponse;
import org.mockito.ArgumentCaptor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class GetPeersRequestProcessorTest extends AbstractCliRequestProcessorTest<GetPeersRequest> {

    @Override
    public GetPeersRequest createRequest(final String groupId, final PeerId peerId) {
        return msgFactory
            .getPeersRequest()
            .groupId(groupId)
            .build();
    }

    @Override
    public BaseCliRequestProcessor<GetPeersRequest> newProcessor() {
        return new GetPeersRequestProcessor(null, msgFactory);
    }

    @Override
    public void verify(final String interest, final Node node, final ArgumentCaptor<Closure> doneArg) {
        assertEquals(interest, GetPeersRequest.class.getName());
        assertNotNull(this.asyncContext.getResponseObject());
        assertEquals("[localhost:8081, localhost:8082, localhost:8083]", this.asyncContext.as(GetPeersResponse.class)
            .peersList().toString());
        assertEquals("[learner:8081, learner:8082, learner:8083]", this.asyncContext.as(GetPeersResponse.class)
            .learnersList().toString());
    }

}
