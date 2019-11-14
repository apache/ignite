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

import java.io.IOException;
import java.lang.reflect.Parameter;
import java.util.UUID;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.ignite.agent.action.ActionMethod;

import static org.apache.ignite.agent.action.annotation.ActionControllerAnnotationReader.actions;

/**
 * Request deserializer.
 */
public class RequestDeserializer extends StdDeserializer<Request> {
    /**
     * Default constructor.
     */
    public RequestDeserializer() {
        super(Request.class);
    }

    /** {@inheritDoc} */
    @Override public Request deserialize(
        JsonParser p,
        DeserializationContext ctxt
    ) throws IOException, JsonProcessingException {
        Request req;

        JsonNode node = p.getCodec().readTree(p);

        UUID id = p.getCodec().treeToValue(node.get("id"), UUID.class);

        UUID sesId = p.getCodec().treeToValue(node.get("sessionId"), UUID.class);

        String act = node.get("action").asText();

        ActionMethod actMtd = actions().get(act);

        try {
            if (actMtd == null)
                throw new IllegalArgumentException("Failed to find method for action: " + act);

            req = new Request().setId(id).setAction(act).setSessionId(sesId);

            Parameter[] parameters = actMtd.method().getParameters();

            if (parameters.length == 1) {
                Class<?> argType = parameters[0].getType();

                Object arg = p.getCodec().treeToValue(node.get("argument"), argType);

                req.setArgument(arg);
            }
        }
        catch (Exception e) {
            req = new InvalidRequest().setCause(e).setId(id).setAction(act);
        }

        return req;
    }
}
