// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.handlers.top;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.affinity.partition.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.port.*;
import org.gridgain.grid.kernal.processors.rest.*;
import org.gridgain.grid.kernal.processors.rest.client.message.*;
import org.gridgain.grid.kernal.processors.rest.handlers.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;

import java.net.*;
import java.util.*;

import static org.gridgain.grid.kernal.GridNodeAttributes.*;

/**
 * Command handler for API requests.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridTopologyCommandHandler extends GridRestCommandHandlerAdapter {
    /**
     * @param ctx Context.
     */
    public GridTopologyCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("fallthrough")
    @Override public boolean supported(GridRestCommand cmd) {
        switch (cmd) {
            case TOPOLOGY:
            case NODE:
                return true;

            default:
                return false;
        }
    }

    /** {@inheritDoc} */
    @Override public GridFuture<GridRestResponse> handleAsync(GridRestRequest req) {
        assert req != null;

        if (log.isDebugEnabled())
            log.debug("Handling topology REST request: " + req);

        GridRestResponse res = new GridRestResponse();

        Object mtrVal = value("mtr", req);
        Object attrVal = value("attr", req);

        boolean mtr = mtrVal != null && (mtrVal instanceof String ?
            Boolean.parseBoolean((String)mtrVal) : (Boolean)mtrVal);
        boolean attr = attrVal != null && (attrVal instanceof String ?
            Boolean.parseBoolean((String)attrVal) : (Boolean)attrVal);

        switch (req.getCommand()) {
            case TOPOLOGY:
                Collection<GridNode> allNodes = F.concat(false,
                    ctx.discovery().allNodes(), ctx.discovery().daemonNodes());

                Collection<GridClientNodeBean> top =
                    new ArrayList<>(allNodes.size());

                for (GridNode node : allNodes)
                    top.add(createNodeBean(node, mtr, attr));

                res.setResponse(top);

                break;

            case NODE:
                String idParam = value("id", req);

                try {
                    UUID id = idParam != null ? UUID.fromString(idParam) : null;

                    final String ip = value("ip", req);

                    if (id == null && ip == null)
                        return new GridFinishedFuture<>(ctx, new GridException(
                            "Failed to handle request (either id or ip should be specified)."));

                    GridNode node;

                    if (id != null) {
                        // Always refresh topology so client see most up-to-date view.
                        ctx.discovery().alive(id);

                        node = ctx.grid().node(id);

                        if (ip != null && node != null && !containsIp(node.addresses(), ip))
                            node = null;
                    }
                    else
                        node = F.find(ctx.discovery().allNodes(), null, new P1<GridNode>() {
                            @Override public boolean apply(GridNode n) {
                                return containsIp(n.addresses(), ip);
                            }
                        });

                    if (node != null)
                        res.setResponse(createNodeBean(node, mtr, attr));
                    else
                        res.setResponse(null);
                }
                catch (IllegalArgumentException e) {
                    String msg = "Failed to parse id parameter [id=" + idParam + ", err=" + e.getMessage() + ']';

                    if (log.isDebugEnabled())
                        log.debug(msg);

                    return new GridFinishedFuture<>(ctx, new GridException(msg));
                }

                break;

            default:
                assert false : "Invalid command for topology handler: " + req;
        }

        if (log.isDebugEnabled())
            log.debug("Handled topology REST request [res=" + res + ", req=" + req + ']');

        return new GridFinishedFuture<>(ctx, res);
    }

    /**
     * @param addrs List of string addresses.
     * @param ip Ip to match.
     * @return Whether {@code ip} present in addresses.
     */
    private boolean containsIp(Iterable<String> addrs, String ip) {
        for (String addr : addrs) {
            try {
                if (InetAddress.getByName(addr).getHostAddress().equals(ip))
                    return true;
            }
            catch (UnknownHostException ignored) {
                // It's ok if we just don't know that host - node could be bound to address in another network.
            }
        }

        return false;
    }

    /**
     * Creates node bean out of grid node. Notice that cache attribute is handled separately.
     *
     * @param node Grid node.
     * @param mtr {@code true} to add metrics.
     * @param attr {@code true} to add attributes.
     * @return Grid Node bean.
     */
    private GridClientNodeBean createNodeBean(GridNode node, boolean mtr, boolean attr) {
        assert node != null;

        GridClientNodeBean nodeBean = new GridClientNodeBean();

        nodeBean.setNodeId(node.id());
        nodeBean.setConsistentId(node.consistentId());
        nodeBean.setJettyPort(attribute(node, ATTR_REST_JETTY_PORT, 0));
        nodeBean.setTcpPort(attribute(node, ATTR_REST_TCP_PORT, 0));

        nodeBean.setTcpAddresses(nonEmptyList(node.<Collection<String>>attribute(ATTR_REST_TCP_ADDRS)));
        nodeBean.setTcpHostNames(nonEmptyList(node.<Collection<String>>attribute(ATTR_REST_TCP_HOST_NAMES)));
        nodeBean.setJettyAddresses(nonEmptyList(node.<Collection<String>>attribute(ATTR_REST_JETTY_ADDRS)));
        nodeBean.setJettyHostNames(nonEmptyList(node.<Collection<String>>attribute(ATTR_REST_JETTY_HOST_NAMES)));

        Integer dfltReplicaCnt = node.attribute(GridCachePartitionAffinityFunction.DFLT_REPLICA_COUNT_ATTR_NAME);

        if (dfltReplicaCnt == null)
            dfltReplicaCnt = GridCachePartitionAffinityFunction.DFLT_REPLICA_COUNT;

        nodeBean.setReplicaCount(dfltReplicaCnt);

        GridCacheAttributes[] caches = node.attribute(ATTR_CACHE);

        if (!F.isEmpty(caches)) {
            assert caches != null;

            Map<String, String> cacheMap = new HashMap<>(caches.length);

            for (GridCacheAttributes cacheAttr : caches) {
                if (cacheAttr.cacheName() != null)
                    cacheMap.put(cacheAttr.cacheName(), cacheAttr.cacheMode().toString());
                else
                    nodeBean.setDefaultCacheMode(cacheAttr.cacheMode().toString());
            }

            nodeBean.setCaches(cacheMap);
        }

        if (mtr) {
            GridNodeMetrics metrics = node.metrics();

            GridClientNodeMetricsBean metricsBean = new GridClientNodeMetricsBean();

            metricsBean.setStartTime(metrics.getStartTime());
            metricsBean.setAverageActiveJobs(metrics.getAverageActiveJobs());
            metricsBean.setAverageCancelledJobs(metrics.getAverageCancelledJobs());
            metricsBean.setAverageCpuLoad(metrics.getAverageCpuLoad());
            metricsBean.setAverageJobExecuteTime(metrics.getAverageJobExecuteTime());
            metricsBean.setAverageJobWaitTime(metrics.getAverageJobWaitTime());
            metricsBean.setAverageRejectedJobs(metrics.getAverageRejectedJobs());
            metricsBean.setAverageWaitingJobs(metrics.getAverageWaitingJobs());
            metricsBean.setCurrentActiveJobs(metrics.getCurrentActiveJobs());
            metricsBean.setCurrentCancelledJobs(metrics.getCurrentCancelledJobs());
            metricsBean.setCurrentCpuLoad(metrics.getCurrentCpuLoad());
            metricsBean.setCurrentGcCpuLoad(metrics.getCurrentGcCpuLoad());
            metricsBean.setCurrentDaemonThreadCount(metrics.getCurrentDaemonThreadCount());
            metricsBean.setCurrentIdleTime(metrics.getCurrentIdleTime());
            metricsBean.setCurrentJobExecuteTime(metrics.getCurrentJobExecuteTime());
            metricsBean.setCurrentJobWaitTime(metrics.getCurrentJobWaitTime());
            metricsBean.setCurrentRejectedJobs(metrics.getCurrentRejectedJobs());
            metricsBean.setCurrentThreadCount(metrics.getCurrentThreadCount());
            metricsBean.setCurrentWaitingJobs(metrics.getCurrentWaitingJobs());
            metricsBean.setHeapMemoryCommitted(metrics.getHeapMemoryCommitted());
            metricsBean.setHeapMemoryInitialized(metrics.getHeapMemoryInitialized());
            metricsBean.setHeapMemoryMaximum(metrics.getHeapMemoryMaximum());
            metricsBean.setHeapMemoryUsed(metrics.getHeapMemoryUsed());
            metricsBean.setLastDataVersion(metrics.getLastDataVersion());
            metricsBean.setLastUpdateTime(metrics.getLastUpdateTime());
            metricsBean.setMaximumActiveJobs(metrics.getMaximumActiveJobs());
            metricsBean.setMaximumCancelledJobs(metrics.getMaximumCancelledJobs());
            metricsBean.setMaximumJobExecuteTime(metrics.getMaximumJobExecuteTime());
            metricsBean.setMaximumJobWaitTime(metrics.getMaximumJobWaitTime());
            metricsBean.setMaximumRejectedJobs(metrics.getMaximumRejectedJobs());
            metricsBean.setMaximumThreadCount(metrics.getMaximumThreadCount());
            metricsBean.setMaximumWaitingJobs(metrics.getMaximumWaitingJobs());
            metricsBean.setNodeStartTime(metrics.getNodeStartTime());
            metricsBean.setNonHeapMemoryCommitted(metrics.getNonHeapMemoryCommitted());
            metricsBean.setNonHeapMemoryInitialized(metrics.getNonHeapMemoryInitialized());
            metricsBean.setNonHeapMemoryMaximum(metrics.getNonHeapMemoryMaximum());
            metricsBean.setNonHeapMemoryUsed(metrics.getNonHeapMemoryUsed());
            metricsBean.setStartTime(metrics.getStartTime());
            metricsBean.setTotalCancelledJobs(metrics.getTotalCancelledJobs());
            metricsBean.setTotalCpus(metrics.getTotalCpus());
            metricsBean.setTotalExecutedJobs(metrics.getTotalExecutedJobs());
            metricsBean.setTotalIdleTime(metrics.getTotalIdleTime());
            metricsBean.setTotalRejectedJobs(metrics.getTotalRejectedJobs());
            metricsBean.setTotalStartedThreadCount(metrics.getTotalStartedThreadCount());
            metricsBean.setTotalExecutedTasks(metrics.getTotalExecutedTasks());
            metricsBean.setSentMessagesCount(metrics.getSentMessagesCount());
            metricsBean.setSentBytesCount(metrics.getSentBytesCount());
            metricsBean.setReceivedMessagesCount(metrics.getReceivedMessagesCount());
            metricsBean.setReceivedBytesCount(metrics.getReceivedBytesCount());
            metricsBean.setUpTime(metrics.getUpTime());

            nodeBean.setMetrics(metricsBean);
        }

        if (attr) {
            Map<String, Object> attrs = new HashMap<>(node.attributes());

            attrs.remove(ATTR_CACHE);

            nodeBean.setAttributes(attrs);
        }

        return nodeBean;
    }

    /**
     * @param col Collection;
     * @return Non-empty list.
     */
    private static Collection<String> nonEmptyList(Collection<String> col) {
        return col == null ? Collections.<String>emptyList() : col;
    }

    /**
     * Get node attribute by specified attribute name.
     *
     * @param node Node to get attribute for.
     * @param attrName Attribute name.
     * @param dfltVal Default result for case when node attribute resolved into {@code null}.
     * @return Attribute value or default result if requested attribute resolved into {@code null}.
     */
    private <T> T attribute(GridNode node, String attrName, T dfltVal) {
        T attr = node.attribute(attrName);

        return attr == null ? dfltVal : attr;
    }

    /**
     * Get registered port
     *
     * @param protoCls Protocol class.
     * @param def Default value if such class is not registered.
     * @return Registered port for the protocol class or {@code def}ault value if such class is not registered.
     */
    private int getRegisteredPort(Class<? extends GridRestProtocol> protoCls, int def) {
        for (GridPortRecord r : ctx.ports().records()) {
            if (r.protocol() == GridPortProtocol.TCP && protoCls.isAssignableFrom(r.clazz()))
                return r.port();
        }

        return def;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTopologyCommandHandler.class, this);
    }
}
