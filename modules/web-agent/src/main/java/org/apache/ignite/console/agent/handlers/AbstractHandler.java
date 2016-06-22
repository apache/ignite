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

package org.apache.ignite.console.agent.handlers;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsonorg.JsonOrgModule;
import io.socket.client.Ack;
import io.socket.emitter.Emitter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Base class for web socket handlers.
 */
abstract class AbstractHandler implements Emitter.Listener {
    /** JSON object mapper. */
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        JsonOrgModule module = new JsonOrgModule();

        mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

        mapper.registerModule(module);
    }

    /**
     * @param obj Object.
     * @return {@link JSONObject} or {@link JSONArray}.
     */
    private Object toJSON(Object obj) {
        if (obj instanceof Iterable)
            return mapper.convertValue(obj, JSONArray.class);

        return mapper.convertValue(obj, JSONObject.class);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public final void call(Object... args) {
        Ack cb = null;

        try {
            if (args == null || args.length == 0)
                throw new IllegalArgumentException("Missing arguments.");

            if (args.length > 2)
                throw new IllegalArgumentException("Wrong arguments count, must be <= 2: " + Arrays.toString(args));

            JSONObject lsnrArgs = null;

            if (args.length == 1) {
                if (args[0] instanceof JSONObject)
                    lsnrArgs = (JSONObject)args[0];
                else if (args[0] instanceof Ack)
                    cb = (Ack)args[0];
                else
                    throw new IllegalArgumentException("Wrong type of argument, must be JSONObject or Ack: " + args[0]);
            }
            else {
                if (args[0] != null && !(args[0] instanceof JSONObject))
                    throw new IllegalArgumentException("Wrong type of argument, must be JSONObject: " + args[0]);

                if (!(args[1] instanceof Ack))
                    throw new IllegalArgumentException("Wrong type of argument, must be Ack: " + args[1]);

                lsnrArgs = (JSONObject)args[0];

                cb = (Ack)args[1];
            }

            Object res = execute(lsnrArgs == null ? Collections.emptyMap() : mapper.convertValue(lsnrArgs, Map.class));

            if (cb != null)
                cb.call(null, toJSON(res));
        }
        catch (Exception e) {
            if (cb != null)
                cb.call(e, null);
        }
    }

    /**
     * Execute command with specified arguments.
     *
     * @param args Map with method args.
     */
    public abstract Object execute(Map<String, Object> args) throws Exception;
}
