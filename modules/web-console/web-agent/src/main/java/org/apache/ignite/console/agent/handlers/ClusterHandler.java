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

import java.net.ConnectException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.agent.rest.RestResult;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.LoggerFactory;

import static org.apache.ignite.console.agent.AgentUtils.sslContextFactory;

/**
 * API to transfer topology from Ignite cluster to Web Console.
 */
public class ClusterHandler extends AbstractClusterHandler {
    /** */
    private static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(ClusterHandler.class));

    /** Index of alive node URI. */
    private final Map<List<String>, Integer> startIdxs = U.newHashMap(2);

    /**
     * @param cfg Web agent configuration.
     */
    public ClusterHandler(AgentConfiguration cfg) {
        super(cfg, createNodeSslFactory(cfg));
    }

    /**
     * @param cfg Config.
     */
    private static SslContextFactory createNodeSslFactory(AgentConfiguration cfg) {
        boolean trustAll = Boolean.getBoolean("trust.all");

        if (trustAll && !F.isEmpty(cfg.nodeTrustStore())) {
            log.warning("Options contains both '--node-trust-store' and '-Dtrust.all=true'. " +
                "Option '-Dtrust.all=true' will be ignored on connect to cluster.");

            trustAll = false;
        }

        boolean ssl = trustAll || !F.isEmpty(cfg.nodeTrustStore()) || !F.isEmpty(cfg.nodeKeyStore());

        if (!ssl)
            return null;

        return sslContextFactory(
            cfg.nodeKeyStore(),
            cfg.nodeKeyStorePassword(),
            trustAll,
            cfg.nodeTrustStore(),
            cfg.nodeTrustStorePassword(),
            cfg.cipherSuites()
        );
    }

    /** {@inheritDoc} */
    @Override public RestResult restCommand(JsonObject params) throws Throwable {
        List<String> nodeURIs = cfg.nodeURIs();

        Integer startIdx = startIdxs.getOrDefault(nodeURIs, 0);

        int urlsCnt = nodeURIs.size();

        for (int i = 0;  i < urlsCnt; i++) {
            int currIdx = (startIdx + i) % urlsCnt;

            String nodeUrl = nodeURIs.get(currIdx);

            try {
                RestResult res = restExecutor.sendRequest(nodeUrl, params);

                // If first attempt failed then throttling should be cleared.
                if (i > 0 || !startIdxs.containsKey(nodeURIs))
                    log.info("Connected to node [url=" + nodeUrl + "]");

                startIdxs.put(nodeURIs, currIdx);

                return res;
            }
            catch (InterruptedException | TimeoutException ignored) {
                // No-op.
            }
            catch (Exception e) {
                LT.error(log, e, "Failed execute request on node [url=" + nodeUrl + ", parameters=" + params + "]");
            }
        }

        LT.warn(log, "Failed to connect to cluster. " +
            "Please ensure that nodes have [ignite-rest-http] module in classpath " +
            "(was copied from libs/optional to libs folder).");

        throw new ConnectException("Failed connect to cluster [urls=" + nodeURIs + ", parameters=" + params + "]");
    }
}
