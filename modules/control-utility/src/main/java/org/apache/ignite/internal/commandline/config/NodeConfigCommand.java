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

package org.apache.ignite.internal.commandline.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.commandline.systemview.SystemViewCommand;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.node.VisorAtomicConfiguration;
import org.apache.ignite.internal.visor.node.VisorBasicConfiguration;
import org.apache.ignite.internal.visor.node.VisorClientConnectorConfiguration;
import org.apache.ignite.internal.visor.node.VisorExecutorServiceConfiguration;
import org.apache.ignite.internal.visor.node.VisorGridConfiguration;
import org.apache.ignite.internal.visor.node.VisorMetricsConfiguration;
import org.apache.ignite.internal.visor.node.VisorNodeConfigurationCollectorTask;
import org.apache.ignite.internal.visor.node.VisorPeerToPeerConfiguration;
import org.apache.ignite.internal.visor.node.VisorRestConfiguration;
import org.apache.ignite.internal.visor.node.VisorSegmentationConfiguration;
import org.apache.ignite.internal.visor.node.VisorSpisConfiguration;
import org.apache.ignite.internal.visor.node.VisorTransactionConfiguration;

import static org.apache.ignite.internal.commandline.CommandList.CONFIG;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTaskByNameOnNode;
import static org.apache.ignite.internal.commandline.config.NodeConfigCommandArg.NODE_ID;
import static org.apache.ignite.internal.visor.systemview.VisorSystemViewTask.SimpleType.STRING;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.NA;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.bool2Str;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactProperty;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.formatMemory;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.safe;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.spiClass;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.spisClass;

/** Represents command for node configuration printing. */
public class NodeConfigCommand extends AbstractCommand<Void> {

    /** ID of the node to get configuration from. */
    private UUID nodeId;

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, IgniteLogger log) throws Exception {
        try {
            VisorGridConfiguration res;

            try (GridClient client = Command.startClient(clientCfg)) {
                res = executeTaskByNameOnNode(
                    client,
                    VisorNodeConfigurationCollectorTask.class.getName(),
                    null,
                    nodeId,
                    clientCfg
                );
            }

            VisorBasicConfiguration basic = res.getBasic();

            VisorAtomicConfiguration atomic = res.getAtomic();
            VisorTransactionConfiguration trn = res.getTransaction();

            log.info("");
            log.info("Common Parameters:");

            SystemViewCommand.printTable(
                null,
                Arrays.asList(STRING, STRING),
                Arrays.asList(
                    Arrays.asList("Grid name", U.toStringSafe(basic.getIgniteInstanceName(), "<default>")),
                    Arrays.asList("Ignite home", safe(basic.getGgHome())),
                    Arrays.asList("Localhost", safe(basic.getLocalHost())),
                    Arrays.asList("Consistent ID", U.toStringSafe(basic.getConsistentId(), "<Not configured explicitly>")),
                    Arrays.asList("Marshaller", basic.getMarshaller()),
                    Arrays.asList("Deployment mode", safe(basic.getDeploymentMode())),
                    Arrays.asList("ClientMode", basic.isClientMode() == null ? "false" : bool2Str(basic.isClientMode())),
                    Arrays.asList("Daemon", bool2Str(basic.isDaemon())),
                    Arrays.asList("Remote JMX enabled", bool2Str(basic.isJmxRemote())),
                    Arrays.asList("Node restart enabled", bool2Str(basic.isRestart())),
                    Arrays.asList("Network timeout", basic.getNetworkTimeout() + "ms"),
                    Arrays.asList("Grid logger", safe(basic.getLogger())),
                    Arrays.asList("Discovery startup delay", basic.getDiscoStartupDelay() + "ms"),
                    Arrays.asList("MBean server", safe(basic.getMBeanServer())),
                    Arrays.asList("ASCII logo disabled", bool2Str(basic.isNoAscii())),
                    Arrays.asList("Discovery order not required", bool2Str(basic.isNoDiscoOrder())),
                    Arrays.asList("Shutdown hook disabled", bool2Str(basic.isNoShutdownHook())),
                    Arrays.asList("Program name", safe(basic.getProgramName())),
                    Arrays.asList("Quiet mode", bool2Str(basic.isQuiet())),
                    Arrays.asList("Success filename", safe(basic.getSuccessFile())),
                    Arrays.asList("Update notification enabled", bool2Str(basic.isUpdateNotifier())),
                    Arrays.asList("Include properties", safe(res.getIncludeProperties())),
                    Arrays.asList("Atomic Cache Mode", atomic.getCacheMode()),
                    Arrays.asList("Atomic Sequence Reservation Size", atomic.getAtomicSequenceReserveSize()),
                    Arrays.asList("Atomic Number Of Backup Nodes", atomic.getBackups()),
                    Arrays.asList("Transaction Concurrency", trn.getDefaultTxConcurrency()),
                    Arrays.asList("Transaction Isolation", trn.getDefaultTxIsolation()),
                    Arrays.asList("Transaction Timeout", trn.getDefaultTxTimeout() + "ms"),
                    Arrays.asList("Transaction Log Cleanup Delay", trn.getPessimisticTxLogLinger() + "ms"),
                    Arrays.asList("Transaction Log Size", trn.getPessimisticTxLogSize()),
                    Arrays.asList("Transaction Manager Factory", trn.getTxManagerFactory()),
                    Arrays.asList("Transaction Use JTA", bool2Str(trn.isUseJtaSync()))
                ),
                log
            );

            log.info("");
            log.info("Metrics:");

            VisorMetricsConfiguration metricsCfg = res.getMetrics();

            long expTime = metricsCfg.getExpireTime();

            SystemViewCommand.printTable(
                null,
                Arrays.asList(STRING, STRING),
                Arrays.asList(
                    Arrays.asList("Metrics expire time", expTime != Long.MAX_VALUE ? expTime + "ms" : "<never>"),
                    Arrays.asList("Metrics history size", metricsCfg.getHistorySize()),
                    Arrays.asList("Metrics log frequency", metricsCfg.getLoggerFrequency())
                ),
                log
            );

            log.info("");
            log.info("SPIs:");

            VisorSpisConfiguration spisCfg = res.getSpis();

            SystemViewCommand.printTable(
                null,
                Arrays.asList(STRING, STRING),
                Arrays.asList(
                    Arrays.asList("Discovery", spiClass(spisCfg.getDiscoverySpi())),
                    Arrays.asList("Communication", spiClass(spisCfg.getCommunicationSpi())),
                    Arrays.asList("Event storage", spiClass(spisCfg.getEventStorageSpi())),
                    Arrays.asList("Collision", spiClass(spisCfg.getCollisionSpi())),
                    Arrays.asList("Deployment", spiClass(spisCfg.getDeploymentSpi())),
                    Arrays.asList("Checkpoints", spisClass(spisCfg.getCheckpointSpis())),
                    Arrays.asList("Failovers", spisClass(spisCfg.getFailoverSpis())),
                    Arrays.asList("Load balancings", spisClass(spisCfg.getLoadBalancingSpis())),
                    Arrays.asList("Indexing", spisClass(spisCfg.getIndexingSpis()))
                ),
                log
            );

            log.info("");
            log.info("Client connector configuration:");

            VisorClientConnectorConfiguration cliConnCfg = res.getClientConnectorConfiguration();

            if (cliConnCfg != null) {
                SystemViewCommand.printTable(
                    null,
                    Arrays.asList(STRING, STRING),
                    Arrays.asList(
                        Arrays.asList("Host", U.toStringSafe(cliConnCfg.getHost(), safe(basic.getLocalHost()))),
                        Arrays.asList("Port", cliConnCfg.getPort()),
                        Arrays.asList("Port range", cliConnCfg.getPortRange()),
                        Arrays.asList("Socket send buffer size", formatMemory(cliConnCfg.getSocketSendBufferSize())),
                        Arrays.asList("Socket receive buffer size", formatMemory(cliConnCfg.getSocketReceiveBufferSize())),
                        Arrays.asList("Max connection cursors", cliConnCfg.getMaxOpenCursorsPerConnection()),
                        Arrays.asList("Pool size", cliConnCfg.getThreadPoolSize()),
                        Arrays.asList("Idle Timeout", cliConnCfg.getIdleTimeout() + "ms"),
                        Arrays.asList("TCP_NODELAY", bool2Str(cliConnCfg.isTcpNoDelay())),
                        Arrays.asList("JDBC Enabled", bool2Str(cliConnCfg.isJdbcEnabled())),
                        Arrays.asList("ODBC Enabled", bool2Str(cliConnCfg.isOdbcEnabled())),
                        Arrays.asList("Thin Client Enabled", bool2Str(cliConnCfg.isThinClientEnabled())),
                        Arrays.asList("SSL Enabled", bool2Str(cliConnCfg.isSslEnabled())),
                        Arrays.asList("Ssl Client Auth", bool2Str(cliConnCfg.isSslClientAuth())),
                        Arrays.asList("Use Ignite SSL Context Factory", bool2Str(cliConnCfg.isUseIgniteSslContextFactory())),
                        Arrays.asList("SSL Context Factory", safe(cliConnCfg.getSslContextFactory()))
                    ),
                    log
                );
            }
            else
                log.info("Client Connection is not configured");

            log.info("");
            log.info("Peer-to-Peer:");

            VisorPeerToPeerConfiguration p2pCfg = res.getP2p();

            SystemViewCommand.printTable(
                null,
                Arrays.asList(STRING, STRING),
                Arrays.asList(
                    Arrays.asList("Peer class loading enabled", bool2Str(p2pCfg.isPeerClassLoadingEnabled())),
                    Arrays.asList("Missed resources cache size", p2pCfg.getPeerClassLoadingMissedResourcesCacheSize()),
                    Arrays.asList("Peer-to-Peer loaded packages", safe(p2pCfg.getPeerClassLoadingLocalClassPathExclude()))
                ),
                log
            );

            log.info("");
            log.info("Lifecycle:");

            SystemViewCommand.printTable(
                null,
                Arrays.asList(STRING, STRING),
                Collections.singletonList(
                    Arrays.asList("Beans", safe(res.getLifecycle().getBeans()))
                ),
                log
            );

            log.info("");
            log.info("Executor services:");

            VisorExecutorServiceConfiguration execCfg = res.getExecutorService();

            SystemViewCommand.printTable(
                null,
                Arrays.asList(STRING, STRING),
                Arrays.asList(
                    Arrays.asList("Public thread pool size", safe(execCfg.getPublicThreadPoolSize())),
                    Arrays.asList("System thread pool size", safe(execCfg.getSystemThreadPoolSize())),
                    Arrays.asList("Management thread pool size", safe(execCfg.getManagementThreadPoolSize())),
                    Arrays.asList("Peer-to-Peer thread pool size", safe(execCfg.getPeerClassLoadingThreadPoolSize())),
                    Arrays.asList("Rebalance Thread Pool size", execCfg.getRebalanceThreadPoolSize()),
                    Arrays.asList("REST thread pool size", safe(execCfg.getRestThreadPoolSize())),
                    Arrays.asList("Client connector thread pool size", safe(execCfg.getClientConnectorConfigurationThreadPoolSize()))
                ),
                log
            );

            log.info("");
            log.info("Segmentation:");

            VisorSegmentationConfiguration segmentationCfg = res.getSegmentation();

            SystemViewCommand.printTable(
                null,
                Arrays.asList(STRING, STRING),
                Arrays.asList(
                    Arrays.asList("Segmentation policy", safe(segmentationCfg.getPolicy())),
                    Arrays.asList("Segmentation resolvers", safe(segmentationCfg.getResolvers())),
                    Arrays.asList("Segmentation check frequency", segmentationCfg.getCheckFrequency()),
                    Arrays.asList("Wait for segmentation on start", bool2Str(segmentationCfg.isWaitOnStart())),
                    Arrays.asList("All resolvers pass required", bool2Str(segmentationCfg.isAllSegmentationResolversPassRequired()))
                ),
                log
            );

            log.info("");
            log.info("Events:");

            String inclEvtTypes;

            if (res.getIncludeEventTypes() == null)
                inclEvtTypes = NA;
            else {
                inclEvtTypes = Arrays.stream(res.getIncludeEventTypes())
                    .mapToObj(U::gridEventName)
                    .collect(Collectors.joining(", "));
            }

            SystemViewCommand.printTable(
                null,
                Arrays.asList(STRING, STRING),
                Collections.singletonList(
                    Arrays.asList("Included event types", inclEvtTypes)
                ),
                log
            );

            log.info("");
            log.info("REST:");

            VisorRestConfiguration restCfg = res.getRest();

            SystemViewCommand.printTable(
                null,
                Arrays.asList(STRING, STRING),
                Arrays.asList(
                    Arrays.asList("REST enabled", bool2Str(restCfg.isRestEnabled())),
                    Arrays.asList("Jetty path", safe(restCfg.getJettyPath())),
                    Arrays.asList("Jetty host", safe(restCfg.getJettyHost())),
                    Arrays.asList("Jetty port", safe(restCfg.getJettyPort())),
                    Arrays.asList("Tcp ssl enabled", bool2Str(restCfg.isTcpSslEnabled())),
                    Arrays.asList("Tcp ssl context factory", safe(restCfg.getTcpSslContextFactory())),
                    Arrays.asList("Tcp host", safe(restCfg.getTcpHost())),
                    Arrays.asList("Tcp port", safe(restCfg.getTcpPort()))
                ),
                log
            );

            log.info("");

            if (!res.getUserAttributes().isEmpty()) {
                log.info("User attributes:");

                List<List<?>> data = new ArrayList<>();

                res.getUserAttributes().forEach((name, value) -> data.add(Arrays.asList(name, value)));

                SystemViewCommand.printTable(
                    Arrays.asList("Name", "Value"),
                    Arrays.asList(STRING, STRING),
                    data,
                    log
                );
            }
            else
                log.info("No user attributes defined.");

            log.info("");

            if (!res.getEnv().isEmpty()) {
                log.info("\nEnvironment variables:");

                //envT.maxCellWidth = 80

                List<List<?>> data = new ArrayList<>();

                res.getEnv().forEach((name, value) -> data.add(Arrays.asList(name, compactProperty(name, value))));

                SystemViewCommand.printTable(
                    Arrays.asList("Name", "Value"),
                    Arrays.asList(STRING, STRING),
                    data,
                    log
                );
            }
            else
                log.info("No environment variables defined.");

            log.info("");

            if (!res.getSystemProperties().isEmpty()) {
                log.info("System properties:");

                List<List<?>> data = new ArrayList<>();

                res.getSystemProperties().forEach(
                    (name, value) -> data.add(Arrays.asList(name, compactProperty(safe(name), safe(value))))
                );

                SystemViewCommand.printTable(
                    Arrays.asList("Name", "Value"),
                    Arrays.asList(STRING, STRING),
                    data,
                    log
                );
            }
            else
                log.info("No system properties defined.");

            return res;
        }
        catch (Throwable e) {
            log.error("Failed to perform operation.");
            log.error(CommandLogger.errorMessage(e));

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        while (argIter.hasNextSubArg()) {
            String arg = argIter.nextArg("Failed to read command argument.");

            NodeConfigCommandArg cmdArg = CommandArgUtils.of(arg, NodeConfigCommandArg.class);

            if (cmdArg == NODE_ID) {
                String nodeIdArg = argIter.nextArg(
                    "ID of the node from which configuration should be obtained is expected.");

                try {
                    nodeId = UUID.fromString(nodeIdArg);
                }
                catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Failed to parse " + NODE_ID + " command argument." +
                        " String representation of \"java.util.UUID\" is exepected. For example:" +
                        " 123e4567-e89b-42d3-a456-556642440000", e);
                }
            }
        }

        if (nodeId == null)
            throw new IllegalArgumentException("node_id required");
    }

    /** {@inheritDoc} */
    @Override public Void arg() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger log) {
        Map<String, String> params = new HashMap<>();

        params.put("node_id", "ID of the node to get config from.");

        usage(log, "Print node configuration:", CONFIG, params, "node_id");
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return CONFIG.toCommandName();
    }
}
