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
import java.nio.channels.AsynchronousCloseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.agent.rest.RestResult;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.eclipse.jetty.client.HttpResponseException;
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
    
    /** Map of clusterId->  node URI. */
    public static final Map<String, List<String>> clusterUrlMap = U.newHashMap(2);

    /**
     * @param cfg Web agent configuration.
     */
    public ClusterHandler(AgentConfiguration cfg) {
        super(cfg, createNodeSslFactory(cfg));
        clusterUrlMap.put("",cfg.nodeURIs());
    }
    
    public static void registerNodeUrl(String clusterId,String url) {
    	List<String> urls = clusterUrlMap.get(clusterId);
    	if(urls==null) {
    		urls = new ArrayList<>(1);
    		urls.add(url);
    		clusterUrlMap.put(clusterId, urls);
    	}
    	else if(!urls.contains(url)){    		
    		urls.add(url);
    	}
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
    @Override public RestResult restCommand(String clusterId,JsonObject params) throws Throwable {
        List<String> nodeURIs = clusterUrlMap.get(clusterId);
        if(nodeURIs==null) {
        	nodeURIs = this.cfg.nodeURIs();
        }

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
            catch (ConnectException | InterruptedException | TimeoutException | HttpResponseException | AsynchronousCloseException ignored) {
                // No-op.
            }
            catch (Throwable e) {
                LT.error(log, e, "Failed execute request on node [url=" + nodeUrl + ", parameters=" + params + "]");

                if (e instanceof SSLException) {
                    LT.warn(log, "Check that connection to cluster node configured correctly.");
                    LT.warn(log, "Options to check: --node-uri, --node-key-store, --node-key-store-password, --node-trust-store, --node-trust-store-password.");
                }
            }
        }

        LT.warn(log, "Failed to connect to cluster.");
        LT.warn(log, "Check that '--node-uri' configured correctly.");
        LT.warn(log, "Ensure that cluster nodes have [ignite-rest-http] module in classpath " +
            "(was copied from libs/optional to libs folder).");

        throw new ConnectException("Failed connect to cluster [urls=" + nodeURIs + ", parameters=" + params + "]");
    }
}
