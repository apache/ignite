

package org.apache.ignite.console.agent.handlers;

import static org.apache.ignite.console.agent.AgentUtils.sslContextFactory;

import java.util.List;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.agent.rest.RestExecutor;
import org.apache.ignite.console.agent.rest.RestResult;
import org.apache.ignite.console.websocket.TopologySnapshot;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;

/**
 * Abstract cluster handler.
 */
public abstract class AbstractClusterHandler implements ClusterHandler {
	/** */
	protected static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(AbstractClusterHandler.class));
    
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
     * @param cfg Config.
     */
    public static SslContextFactory createNodeSslFactory(AgentConfiguration cfg) {
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

    /**
     * Execute REST command under agent user.
     *
     * @param params Command params.
     * @return Command result.
     * @throws Exception If failed to execute.
     */
    public abstract RestResult restCommand(String clusterId,JsonObject params) throws Throwable;
    
    public abstract List<TopologySnapshot> topologySnapshot();
    
    public void close() {
    	
    }
}
