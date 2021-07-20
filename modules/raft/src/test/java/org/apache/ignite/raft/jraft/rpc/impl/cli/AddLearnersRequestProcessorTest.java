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

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.rpc.CliRequests.AddLearnersRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.LearnersOpResponse;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;

public class AddLearnersRequestProcessorTest extends AbstractCliRequestProcessorTest<AddLearnersRequest> {

    @Override
    public AddLearnersRequest createRequest(final String groupId, final PeerId peerId) {
        return msgFactory
            .addLearnersRequest()
            .groupId(groupId)
            .leaderId(peerId.toString())
            .learnersList(List.of("learner:8082", "test:8182", "test:8183"))
            .build();
    }

    @Override
    public BaseCliRequestProcessor<AddLearnersRequest> newProcessor() {
        return new AddLearnersRequestProcessor(null, msgFactory);
    }

    @Override
    public void verify(final String interest, final Node node, final ArgumentCaptor<Closure> doneArg) {
        assertEquals(AddLearnersRequest.class.getName(), interest);
        Mockito.verify(node).addLearners(
            eq(Arrays.asList(new PeerId("learner", 8082), new PeerId("test", 8182), new PeerId("test", 8183))),
            doneArg.capture());
        Closure done = doneArg.getValue();
        assertNotNull(done);
        done.run(Status.OK());
        assertNotNull(this.asyncContext.getResponseObject());
        assertEquals("[learner:8081, learner:8082, learner:8083]", this.asyncContext.as(LearnersOpResponse.class)
            .oldLearnersList().toString());
        assertEquals("[learner:8081, learner:8082, learner:8083, test:8182, test:8183]",
            this.asyncContext.as(LearnersOpResponse.class).newLearnersList().toString());
    }

}
