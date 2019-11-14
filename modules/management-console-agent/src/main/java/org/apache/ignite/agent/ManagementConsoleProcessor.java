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

package org.apache.ignite.agent;

import java.io.EOFException;
import java.lang.reflect.Type;
import java.net.ConnectException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.agent.action.SessionRegistry;
import org.apache.ignite.agent.dto.action.Request;
import org.apache.ignite.agent.processor.ActionsProcessor;
import org.apache.ignite.agent.processor.CacheChangesProcessor;
import org.apache.ignite.agent.processor.ClusterInfoProcessor;
import org.apache.ignite.agent.processor.ManagementConsoleMessagesProcessor;
import org.apache.ignite.agent.processor.export.EventsExporter;
import org.apache.ignite.agent.processor.export.MetricsExporter;
import org.apache.ignite.agent.processor.export.NodesConfigurationExporter;
import org.apache.ignite.agent.processor.export.SpanExporter;
import org.apache.ignite.agent.processor.metrics.MetricsProcessor;
import org.apache.ignite.agent.ws.WebSocketManager;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.cluster.IgniteClusterImpl;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.management.ManagementConfiguration;
import org.apache.ignite.internal.processors.management.ManagementConsoleProcessorAdapter;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.metastorage.ReadableDistributedMetaStorage;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.eclipse.jetty.websocket.api.UpgradeException;
import org.springframework.messaging.simp.stomp.ConnectionLostException;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import static java.util.Collections.singletonList;
import static org.apache.ignite.agent.StompDestinationsUtils.buildActionRequestTopic;
import static org.apache.ignite.agent.StompDestinationsUtils.buildMetricsPullTopic;
import static org.apache.ignite.agent.utils.AgentUtils.monitoringUri;
import static org.apache.ignite.agent.utils.AgentUtils.quiteStop;
import static org.apache.ignite.agent.utils.AgentUtils.toWsUri;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_NODE_SEGMENTED;
import static org.apache.ignite.internal.IgniteFeatures.CLUSTER_ID_AND_TAG;
import static org.apache.ignite.internal.IgniteFeatures.allNodesSupports;
import static org.apache.ignite.internal.util.IgniteUtils.isLocalNodeCoordinator;

/**
 * Management console agent.
 */
public class ManagementConsoleProcessor extends ManagementConsoleProcessorAdapter {
    /** Management Console configuration meta storage prefix. */
    private static final String MANAGEMENT_CFG_META_STORAGE_PREFIX = "mgmt-console-cfg";

    /** Topic management console. */
    public static final String TOPIC_MANAGEMENT_CONSOLE = "mgmt-console-topic";

    /** Discovery event on restart agent. */
    private static final int[] EVTS_DISCOVERY = new int[] {EVT_NODE_FAILED, EVT_NODE_LEFT, EVT_NODE_SEGMENTED};

    /** Websocket manager. */
    private WebSocketManager mgr;

    /** Cluster processor. */
    private ClusterInfoProcessor clusterProc;

    /** Span exporter. */
    private SpanExporter spanExporter;

    /** Events exporter. */
    private EventsExporter evtsExporter;

    /** Metric exporter. */
    private MetricsExporter metricExporter;

    /** Metric processor. */
    private MetricsProcessor metricProc;

    /** Actions processor. */
    private ActionsProcessor actProc;

    /** Topic processor. */
    private ManagementConsoleMessagesProcessor messagesProc;

    /** Cache processor. */
    private CacheChangesProcessor cacheProc;

    /** Execute service. */
    private ExecutorService connectPool;

    /** Meta storage. */
    private DistributedMetaStorage metaStorage;

    /** Session registry. */
    private SessionRegistry sesRegistry;

    /** Active server uri. */
    private String curSrvUri;

    /** If first connection error after successful connection. */
    private AtomicBoolean disconnected = new AtomicBoolean();

    /** Is all features enabled. */
    private boolean isMgmtConsoleFeaturesEnabled;

    /**
     * @param ctx Kernal context.
     */
    public ManagementConsoleProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) {
        isMgmtConsoleFeaturesEnabled = ReadableDistributedMetaStorage.isSupported(ctx) &&
            allNodesSupports(ctx, ctx.discovery().discoCache().allNodes(), CLUSTER_ID_AND_TAG);

        if (isMgmtConsoleFeaturesEnabled) {
            this.metaStorage = ctx.distributedMetastorage();
            this.evtsExporter = new EventsExporter(ctx);
            this.spanExporter = new SpanExporter(ctx);
            this.metricExporter = new MetricsExporter(ctx);

            // Connect to backend if local node is a coordinator or await coordinator change event.
            if (isLocalNodeCoordinator(ctx.discovery())) {
                messagesProc = new ManagementConsoleMessagesProcessor(ctx);

                connect();
            }
            else
                ctx.event().addDiscoveryEventListener(this::launchAgentListener, EVTS_DISCOVERY);

            evtsExporter.addLocalEventListener();

            metricExporter.addMetricListener();

            NodesConfigurationExporter exporter = new NodesConfigurationExporter(ctx);

            exporter.export();

            quiteStop(exporter);
        }
        else
            log.warning("Management console requires DISTRIBUTED_METASTORAGE and CLUSTER_ID_AND_TAG features for work");
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        if (isMgmtConsoleFeaturesEnabled) {
            ctx.event().removeDiscoveryEventListener(this::launchAgentListener, EVTS_DISCOVERY);

            quiteStop(messagesProc);
            quiteStop(metricExporter);
            quiteStop(evtsExporter);
            quiteStop(spanExporter);

            disconnect();
        }
    }

    /**
     *  Stop agent.
     */
    private void disconnect() {
        log.info("Stopping Management Console agent.");

        U.shutdownNow(getClass(), connectPool, log);

        quiteStop(cacheProc);
        quiteStop(actProc);
        quiteStop(metricProc);
        quiteStop(clusterProc);
        quiteStop(mgr);

        disconnected.set(false);

        U.quietAndInfo(log, "Management console agent stopped.");
    }

    /** {@inheritDoc} */
    @Override public void configuration(ManagementConfiguration cfg) {
        if (isMgmtConsoleFeaturesEnabled) {
            ManagementConfiguration oldCfg = configuration();

            if (oldCfg.isEnabled() != cfg.isEnabled())
                cfg = oldCfg.setEnabled(cfg.isEnabled());

            super.configuration(cfg);

            writeToMetaStorage(cfg);

            disconnect();

            launchAgentListener(null, null);
        }
    }

    /**
     * @return Session registry.
     */
    public SessionRegistry sessionRegistry() {
        return sesRegistry;
    }

    /**
     * @return Weboscket manager.
     */
    public WebSocketManager webSocketManager() {
        return mgr;
    }

    /**
     * Start agent on local node if this is coordinator node.
     */
    private void launchAgentListener(DiscoveryEvent evt, DiscoCache discoCache) {
        if (isLocalNodeCoordinator(ctx.discovery())) {
            cfg = readFromMetaStorage();

            connect();
        }
    }

    /**
     * @param uris Management Console Server URIs.
     */
    private String nextUri(List<String> uris, String cur) {
        int idx = uris.indexOf(cur);

        return uris.get((idx + 1) % uris.size());
    }

    /**
     * Connect to backend in same thread.
     */
    private void connect0() {
        while (!ctx.isStopping()) {
            try {
                mgr.stop(true);

                curSrvUri = nextUri(cfg.getConsoleUris(), curSrvUri);

                mgr.connect(toWsUri(curSrvUri), cfg, new AfterConnectedSessionHandler());

                disconnected.set(false);

                break;
            }
            catch (Exception e) {
                mgr.stop(true);

                if (X.hasCause(e, InterruptedException.class)) {
                    U.quiet(true, "Caught interrupted exception: " + e);

                    Thread.currentThread().interrupt();

                    break;
                }
                else if (X.hasCause(e, TimeoutException.class, ConnectException.class, UpgradeException.class,
                    EOFException.class, ConnectionLostException.class)) {
                    if (disconnected.compareAndSet(false, true))
                        log.error("Failed to establish websocket connection with Management Console: " + curSrvUri);
                }
                else
                    log.error("Failed to establish websocket connection with Management Console: " + curSrvUri, e);
            }
        }
    }

    /**
     * Connect to backend.
     */
    private void connect() {
        if (!cfg.isEnabled()) {
            log.info("Skip start Management Console agent on coordinator, because it was disabled in configuration");

            return;
        }

        log.info("Starting Management Console agent on coordinator");

        this.mgr = new WebSocketManager(ctx);
        this.sesRegistry = new SessionRegistry(ctx);
        this.clusterProc = new ClusterInfoProcessor(ctx, mgr);
        this.metricProc = new MetricsProcessor(ctx, mgr);
        this.actProc = new ActionsProcessor(ctx, mgr);
        this.cacheProc = new CacheChangesProcessor(ctx, mgr);

        evtsExporter.addGlobalEventListener();

        connectPool = Executors.newSingleThreadExecutor(new CustomizableThreadFactory("mgmt-console-connection-"));

        connectPool.submit(this::connect0);
    }

    /**
     * @return Agent configuration.
     */
    private ManagementConfiguration readFromMetaStorage() {
        if (metaStorage == null)
            return new ManagementConfiguration();

        ctx.cache().context().database().checkpointReadLock();

        ManagementConfiguration cfg;

        try {
            cfg = metaStorage.read(MANAGEMENT_CFG_META_STORAGE_PREFIX);
        }
        catch (IgniteCheckedException e) {
            log.warning("Failed to read management configuration from meta storage!");

            throw U.convertException(e);
        }
        finally {
            ctx.cache().context().database().checkpointReadUnlock();
        }

        return cfg != null ? cfg : new ManagementConfiguration();
    }

    /**
     * @param cfg Agent configuration.
     */
    private void writeToMetaStorage(ManagementConfiguration cfg) {
        ctx.cache().context().database().checkpointReadLock();

        try {
            metaStorage.write(MANAGEMENT_CFG_META_STORAGE_PREFIX, cfg);
        }
        catch (IgniteCheckedException e) {
            log.warning("Failed to save management configuration to meta storage!");

            throw U.convertException(e);
        }
        finally {
            ctx.cache().context().database().checkpointReadUnlock();
        }
    }

    /**
     * Session handler for sending cluster info to backend.
     */
    private class AfterConnectedSessionHandler extends StompSessionHandlerAdapter {
        /** {@inheritDoc} */
        @Override public void afterConnected(StompSession ses, StompHeaders stompHeaders) {
            IgniteClusterImpl cluster = ctx.cluster().get();

            U.quietAndInfo(log, "");

            U.quietAndInfo(log, "Found Management Console that can be used to monitor your cluster: " + curSrvUri);

            U.quietAndInfo(log, "");

            U.quietAndInfo(log, "Open link in browser to monitor your cluster: " +
                    monitoringUri(curSrvUri, cluster.id()));

            U.quietAndInfo(log, "If you already using Management Console, you can add cluster manually by it's ID: " + cluster.id());

            clusterProc.sendInitialState();

            cacheProc.sendInitialState();

            ses.subscribe(buildMetricsPullTopic(), new StompFrameHandler() {
                /** {@inheritDoc} */
                @Override public Type getPayloadType(StompHeaders headers) {
                    return String.class;
                }

                /** {@inheritDoc} */
                @Override public void handleFrame(StompHeaders headers, Object payload) {
                    metricProc.broadcastPullMetrics();
                }
            });

            ses.subscribe(buildActionRequestTopic(cluster.id()), new StompFrameHandler() {
                /** {@inheritDoc} */
                @Override public Type getPayloadType(StompHeaders headers) {
                    return Request.class;
                }

                /** {@inheritDoc} */
                @Override public void handleFrame(StompHeaders headers, Object payload) {
                    actProc.onActionRequest((Request)payload);
                }
            });

            cfg.setConsoleUris(singletonList(curSrvUri));

            writeToMetaStorage(cfg);
        }

        /** {@inheritDoc} */
        @Override public void handleException(StompSession ses, StompCommand cmd, StompHeaders headers, byte[] payload, Throwable e) {
            log.warning("Failed to process a STOMP frame", e);
        }

        /** {@inheritDoc} */
        @Override public void handleTransportError(StompSession stompSes, Throwable e) {
            if (e instanceof ConnectionLostException) {
                if (disconnected.compareAndSet(false, true)) {
                    log.error("Lost websocket connection with server: " + curSrvUri);

                    reconnect();
                }
            }
        }
    }

    /**
     * Submit a reconnection task.
     */
    private void reconnect() {
        connectPool.submit(this::connect0);
    }
}
