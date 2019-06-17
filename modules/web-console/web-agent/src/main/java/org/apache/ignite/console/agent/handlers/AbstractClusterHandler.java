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

package org.apache.ignite.console.agent.handlers;

import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.agent.rest.RestExecutor;
import org.apache.ignite.console.agent.rest.RestResult;
import org.apache.ignite.console.json.JsonObject;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * Abstract cluster handler.
 */
public abstract class AbstractClusterHandler {
    /** Agent configuration. */
    protected final AgentConfiguration cfg;

    /** Rest executor. */
    protected final RestExecutor restExecutor;

    /**
     * @param cfg Web agent configuration.
     */
    AbstractClusterHandler(AgentConfiguration cfg, SslContextFactory sslCtxFactory) {
        this.cfg = cfg;

        restExecutor = new RestExecutor(sslCtxFactory);
    }

    /**
     * Execute REST command under agent user.
     *
     * @param params Command params.
     * @return Command result.
     * @throws Exception If failed to execute.
     */
    public abstract RestResult restCommand(JsonObject params) throws Throwable;
}
