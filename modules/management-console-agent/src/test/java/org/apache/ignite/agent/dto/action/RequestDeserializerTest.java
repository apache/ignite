/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.dto.action;

import java.util.UUID;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.apache.ignite.agent.utils.AgentObjectMapperFactory.binaryMapper;
import static org.junit.Assert.assertTrue;

/**
 * Request deserializer test.
 */
public class RequestDeserializerTest {
    /** Mapper. */
    private final ObjectMapper mapper = binaryMapper();

    /**
     * Should set ResponseError to request argument.
     */
    @Test
    public void deserializeWithInvalidAction() throws Exception {
        Request req = new Request().setAction("InvalidAction").setArgument(false).setId(UUID.randomUUID());

        Request deserializedReq = mapper.readValue(mapper.writeValueAsBytes(req), Request.class);

        assertTrue(deserializedReq instanceof InvalidRequest);
    }

    /**
     * Should correct deserialize action request.
     */
    @Test
    public void deserializeWithValidAction() throws Exception {
        Request req = new Request().setAction("IgniteTestActionController.action").setArgument(true).setId(UUID.randomUUID());

        Request deserializedReq = mapper.readValue(mapper.writeValueAsBytes(req), Request.class);

        assertTrue((boolean) deserializedReq.getArgument());
    }

    /**
     * Should set ResponseError to request argument.
     */
    @Test
    public void deserializeWithInvalidArgument() throws Exception {
        Request req = new Request().setAction("ActionControllerForTests.numberAction").setArgument("number").setId(UUID.randomUUID());

        Request deserializedReq = mapper.readValue(mapper.writeValueAsBytes(req), Request.class);

        assertTrue(deserializedReq instanceof InvalidRequest);
    }
}
