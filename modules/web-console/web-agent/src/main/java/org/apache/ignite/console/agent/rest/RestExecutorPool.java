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

package org.apache.ignite.console.agent.rest;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.console.agent.AgentConfiguration;

/**
 * Class that store collection of {@link RestExecutor}.
 */
public class RestExecutorPool implements AutoCloseable {
    /** */
    private final Map<String, RestExecutor> pool = new HashMap<>();

    /** */
    private final AgentConfiguration cfg;

    /**
     * @param cfg Agent configuration.
     */
    public RestExecutorPool(AgentConfiguration cfg) {
        this.cfg = cfg;
    }

    /** */
    public RestResult sendRequest(
        String name,
        List<String> nodeURIs,
        Map<String, Object> params,
        Map<String, Object> headers
    ) throws IOException {
        RestExecutor restExec = pool.get(name);

        if (restExec == null) {
            restExec = new RestExecutor(cfg);

            pool.put(name, restExec);
        }

        return restExec.sendRequest(nodeURIs, params, headers);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        pool.values().forEach(RestExecutor::close);
        pool.clear();
    }
}
