

package org.apache.ignite.console.agent.handlers;


import static org.apache.ignite.console.utils.Utils.fromJson;
import static org.apache.ignite.console.websocket.WebSocketEvents.CLUSTER_TOPOLOGY;
import static org.apache.ignite.internal.processors.rest.GridRestResponse.STATUS_SUCCESS;
import static org.apache.ignite.internal.processors.rest.client.message.GridClientResponse.STATUS_FAILED;
import static org.apache.ignite.lang.IgniteProductVersion.fromString;

import java.io.Closeable;
import java.net.ConnectException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.agent.AgentUtils;
import org.apache.ignite.console.agent.rest.RestResult;
import org.apache.ignite.console.websocket.TopologySnapshot;
import org.apache.ignite.console.websocket.WebSocketResponse;


import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.logger.slf4j.Slf4jLogger;
import org.eclipse.jetty.websocket.api.Session;
import org.slf4j.LoggerFactory;
import io.vertx.core.json.JsonObject;

/**
 * API to transfer topology from Ignite cluster to Web Console.
 */
public class ClustersWatcher implements Closeable {
    /** */
	public static final IgniteLogger log = new Slf4jLogger(LoggerFactory.getLogger(ClustersWatcher.class));

    /** */
	public static final IgniteProductVersion IGNITE_2_0 = fromString("2.0.0");

    /** */
	public static final IgniteProductVersion IGNITE_2_3 = fromString("2.3.0");  
    
    /**My Extended Ignite */
	public static final IgniteProductVersion IGNITE_2_16 = fromString("2.16.999");
    

    /** Topology refresh frequency. */
    private static final long REFRESH_FREQ = 10_000L;

    
    /** Agent configuration. */
    private AgentConfiguration cfg;    

    /** Cluster handler. */
    private ClusterHandler[] clusterHnds;    



    /** Executor pool. */
    private final ScheduledExecutorService pool = Executors.newScheduledThreadPool(1);

    /** Refresh task. */
    private ScheduledFuture<?> refreshTask;


    /**
     * @param cfg Agent configuration.
     * @param clusterHnd Cluster handler.
     */
    ClustersWatcher(AgentConfiguration cfg, ClusterHandler... clusterHnds) {
        this.cfg = cfg;
        this.clusterHnds = clusterHnds;        
    }

    /**
     * Send event to websocket.
     *
     * @param ses Websocket session.
     * @param tops Topologies.
     */
    private void sendTopology(Session ses, List<TopologySnapshot> tops) {
        try {
        	AgentUtils.send(ses, new WebSocketResponse(CLUSTER_TOPOLOGY, tops), 10L, TimeUnit.SECONDS);
        }
        catch (Throwable e) {
            log.error("Failed to send topology to server");
        }
    }

    /**
     * Start watch cluster.
     */
    void startWatchTask(Session ses) {
        if (refreshTask != null && !refreshTask.isCancelled()) {
            log.warning("Detected that watch task already running");

            refreshTask.cancel(true);
        }

        refreshTask = pool.scheduleWithFixedDelay(() -> {
        	List<TopologySnapshot> tops = new LinkedList<TopologySnapshot>();
        	for(ClusterHandler hnd: clusterHnds) {
	        	
        		List<TopologySnapshot>  ctops = hnd.topologySnapshot();
        		if(ctops!=null) {
        			tops.addAll(ctops);
        		}
        	}
            
            sendTopology(ses, tops);
            
        }, 0L, REFRESH_FREQ, TimeUnit.MILLISECONDS);
    }

    /**
     * Stop cluster watch.
     */
    void stop() {
        if (refreshTask != null) {
            refreshTask.cancel(true);

            refreshTask = null;

            log.info("Topology watch process was suspended");
        }
    }

    

    /** {@inheritDoc} */
    @Override public void close() {
        if (refreshTask != null)
            refreshTask.cancel(true);

        pool.shutdownNow();
    }
}
