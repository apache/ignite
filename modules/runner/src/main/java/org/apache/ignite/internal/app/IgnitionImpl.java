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

package org.apache.ignite.internal.app;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.app.Ignition;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.schemas.network.NetworkConfiguration;
import org.apache.ignite.configuration.schemas.network.NetworkView;
import org.apache.ignite.configuration.schemas.rest.RestConfiguration;
import org.apache.ignite.configuration.schemas.runner.ClusterConfiguration;
import org.apache.ignite.configuration.schemas.runner.NodeConfiguration;
import org.apache.ignite.configuration.schemas.table.TableValidator;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.affinity.AffinityManager;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.processors.query.calcite.SqlQueryProcessor;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.configuration.SchemaTableValidatorImpl;
import org.apache.ignite.internal.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.storage.LocalConfigurationStorage;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.VaultService;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.LoggerMessageHelper;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.network.scalecube.ScaleCubeClusterServiceFactory;
import org.apache.ignite.rest.RestModule;
import org.apache.ignite.utils.IgniteProperties;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of an entry point for handling grid lifecycle.
 */
public class IgnitionImpl implements Ignition {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(IgnitionImpl.class);

    /**
     * Path to the persistent storage used by the {@link VaultService} component.
     */
    private static final Path VAULT_DB_PATH = Paths.get("vault");

    /**
     * Path for the partitions persistent storage.
     */
    private static final Path PARTITIONS_STORE_PATH = Paths.get("db");

    /** */
    private static final String[] BANNER = {
        "",
        "           #              ___                         __",
        "         ###             /   |   ____   ____ _ _____ / /_   ___",
        "     #  #####           / /| |  / __ \\ / __ `// ___// __ \\ / _ \\",
        "   ###  ######         / ___ | / /_/ // /_/ // /__ / / / // ___/",
        "  #####  #######      /_/  |_|/ .___/ \\__,_/ \\___//_/ /_/ \\___/",
        "  #######  ######            /_/",
        "    ########  ####        ____               _  __           _____",
        "   #  ########  ##       /  _/____ _ ____   (_)/ /_ ___     |__  /",
        "  ####  #######  #       / / / __ `// __ \\ / // __// _ \\     /_ <",
        "   #####  #####        _/ / / /_/ // / / // // /_ / ___/   ___/ /",
        "     ####  ##         /___/ \\__, //_/ /_//_/ \\__/ \\___/   /____/",
        "       ##                  /____/\n"
    };

    /** */
    private static final String VER_KEY = "version";

    /** Mapping of a node name to a node status, e.g. node0 -> starting or node1 -> stopping. */
    private static Map<String, NodeState> nodesStatus = new ConcurrentHashMap<>();

    /**
     * Mapping of a node name to a started node components list.
     * Given map helps to stop node by stopping all it's components in an appropriate order both
     * when node is already started which means that all components are ready and
     * if node is in a middle of a startup process which means that only part of it's components are prepared.
     */
    private static Map<String, List<IgniteComponent>> nodesStartedComponents = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public Ignite start(@NotNull String nodeName, @Nullable Path cfgPath, @NotNull Path workDir) {
        try {
            return doStart(
                nodeName,
                cfgPath == null ? null : Files.readString(cfgPath),
                workDir
            );
        }
        catch (IOException e) {
            LOG.warn("Unable to read user specific configuration, default configuration will be used: " + e.getMessage());
            return start(nodeName, workDir);
        }
    }

    /** {@inheritDoc} */
    @Override public Ignite start(@NotNull String name, @Nullable InputStream config, @NotNull Path workDir) {
        try {
            return doStart(
                name,
                config == null ? null : new String(config.readAllBytes(), StandardCharsets.UTF_8),
                workDir
            );
        }
        catch (IOException e) {
            LOG.warn("Unable to read user specific configuration, default configuration will be used: " + e.getMessage());
            return start(name, workDir);
        }
    }

    /** {@inheritDoc} */
    @Override public Ignite start(@NotNull String name, @NotNull Path workDir) {
        return doStart(name, null, workDir);
    }

    /** {@inheritDoc} */
    @Override public void stop(@NotNull String name) {
        AtomicBoolean explicitStop = new AtomicBoolean();

        nodesStatus.computeIfPresent(name, (nodeName, state) -> {
            if (state == NodeState.STARTED)
                explicitStop.set(true);

            return NodeState.STOPPING;
        });

        if (explicitStop.get()) {
            List<IgniteComponent> startedComponents = nodesStartedComponents.get(name);

            doStopNode(name, startedComponents);
        }
    }

    /**
     * Starts an Ignite node with an optional bootstrap configuration from a HOCON file.
     *
     * @param nodeName Name of the node. Must not be {@code null}.
     * @param cfgContent Node configuration in the HOCON format. Can be {@code null}.
     * @param workDir Work directory for the started node. Must not be {@code null}.
     * @return Started Ignite node.
     */
    private static Ignite doStart(String nodeName, @Nullable String cfgContent, Path workDir) {
        if (nodeName.isEmpty())
            throw new IllegalArgumentException("Node name must not be null or empty.");

        NodeState prevNodeState = nodesStatus.putIfAbsent(nodeName, NodeState.STARTING);

        if (prevNodeState != null) {
            String errMsg = "Node with name=[" + nodeName + "] already exists in state=[" + prevNodeState + "].";

            LOG.error(errMsg);

            throw new IgniteException(errMsg);
        }

        ackBanner();

        List<IgniteComponent> startedComponents = new ArrayList<>();

        try {
            // Vault startup.
            VaultManager vaultMgr = doStartComponent(
                nodeName,
                startedComponents,
                createVault(workDir)
            );

            vaultMgr.putName(nodeName).join();

            List<RootKey<?, ?>> nodeRootKeys = List.of(
                NetworkConfiguration.KEY,
                NodeConfiguration.KEY,
                RestConfiguration.KEY
            );

            // Bootstrap node configuration manager.
            ConfigurationManager nodeCfgMgr = doStartComponent(
                nodeName,
                startedComponents,
                new ConfigurationManager(
                    nodeRootKeys,
                    Map.of(),
                    new LocalConfigurationStorage(vaultMgr)
                )
            );

            if (cfgContent != null) {
                try {
                    nodeCfgMgr.bootstrap(cfgContent);
                }
                catch (Exception e) {
                    LOG.warn("Unable to parse user-specific configuration, default configuration will be used: {}", e.getMessage());
                }
            }
            else
                nodeCfgMgr.configurationRegistry().initializeDefaults();

            NetworkView netConfigurationView =
                nodeCfgMgr.configurationRegistry().getConfiguration(NetworkConfiguration.KEY).value();

            var serializationRegistry = new MessageSerializationRegistryImpl();

            var nodeFinder = StaticNodeFinder.fromConfiguration(netConfigurationView);

            // Network startup.
            ClusterService clusterNetSvc = doStartComponent(
                nodeName,
                startedComponents,
                new ScaleCubeClusterServiceFactory().createClusterService(
                    new ClusterLocalConfiguration(
                        nodeName,
                        netConfigurationView.port(),
                        nodeFinder,
                        serializationRegistry
                    )
                )
            );

            // Raft Component startup.
            Loza raftMgr = doStartComponent(
                nodeName,
                startedComponents,
                new Loza(clusterNetSvc, workDir)
            );

            // Meta storage Component startup.
            MetaStorageManager metaStorageMgr = doStartComponent(
                nodeName,
                startedComponents,
                new MetaStorageManager(
                    vaultMgr,
                    nodeCfgMgr,
                    clusterNetSvc,
                    raftMgr
                )
            );

            // TODO IGNITE-14578 Bootstrap configuration manager with distributed configuration.

            List<RootKey<?, ?>> clusterRootKeys = List.of(
                ClusterConfiguration.KEY,
                TablesConfiguration.KEY
            );

            // Start cluster configuration manager.
            ConfigurationManager clusterCfgMgr = doStartComponent(
                nodeName,
                startedComponents,
                new ConfigurationManager(
                    clusterRootKeys,
                    Map.of(TableValidator.class, Set.of(SchemaTableValidatorImpl.INSTANCE)),
                    new DistributedConfigurationStorage(metaStorageMgr, vaultMgr)
                )
            );

            // Baseline manager startup.
            BaselineManager baselineMgr = doStartComponent(
                nodeName,
                startedComponents,
                new BaselineManager(
                    clusterCfgMgr,
                    metaStorageMgr,
                    clusterNetSvc
                )
            );

            // Affinity manager startup.
            AffinityManager affinityMgr = doStartComponent(
                nodeName,
                startedComponents,
                new AffinityManager(
                    clusterCfgMgr,
                    metaStorageMgr,
                    baselineMgr
                )
            );

            // Schema manager startup.
            SchemaManager schemaMgr = doStartComponent(
                nodeName,
                startedComponents,
                new SchemaManager(
                    clusterCfgMgr,
                    metaStorageMgr,
                    vaultMgr
                )
            );

            // Distributed table manager startup.
            TableManager distributedTblMgr = doStartComponent(
                nodeName,
                startedComponents,
                new TableManager(
                    nodeCfgMgr,
                    clusterCfgMgr,
                    metaStorageMgr,
                    schemaMgr,
                    affinityMgr,
                    raftMgr,
                    getPartitionsStorePath(workDir)
                )
            );

            SqlQueryProcessor qryProc = doStartComponent(
                nodeName,
                startedComponents,
                new SqlQueryProcessor(
                    clusterNetSvc,
                    distributedTblMgr
                )
            );

            doStartComponent(
                nodeName,
                startedComponents,
                new RestModule(nodeCfgMgr, clusterCfgMgr)
            );

            // Deploy all resisted watches cause all components are ready and have registered their listeners.
            metaStorageMgr.deployWatches();

            AtomicBoolean explicitStop = new AtomicBoolean();

            nodesStatus.computeIfPresent(nodeName, (name, state) -> {
                switch (state) {
                    case STARTING:
                        nodesStartedComponents.put(name, startedComponents);

                        return NodeState.STARTED;
                    case STOPPING:
                        explicitStop.set(true);
                }

                return state;
            });

            if (explicitStop.get())
                throw new NodeStoppingException();

            ackSuccessStart();

            return new IgniteImpl(
                nodeName,
                distributedTblMgr,
                qryProc,
                nodeCfgMgr,
                clusterCfgMgr
            );
        }
        catch (Exception e) {
            String errMsg = "Unable to start node=[" + nodeName + "].";

            LOG.error(errMsg, e);

            doStopNode(nodeName, startedComponents);

            throw new IgniteException(errMsg, e);
        }
    }

    /**
     * Returns a path to the partitions store directory.
     * Creates a directory if it doesn't exist.
     *
     * @param workDir Ignite work directory.
     * @return Partitions store path.
     */
    @NotNull
    private static Path getPartitionsStorePath(Path workDir) {
        Path partitionsStore = workDir.resolve(PARTITIONS_STORE_PATH);

        try {
            Files.createDirectories(partitionsStore);
        } catch (IOException e) {
            throw new IgniteInternalException("Failed to create directory for partitions storage: " + e.getMessage(), e);
        }

        return partitionsStore;
    }

    /**
     * Starts the Vault component.
     */
    private static VaultManager createVault(Path workDir) {
        Path vaultPath = workDir.resolve(VAULT_DB_PATH);

        try {
            Files.createDirectories(vaultPath);
        }
        catch (IOException e) {
            throw new IgniteInternalException(e);
        }

        var vaultMgr = new VaultManager(new PersistentVaultService(vaultPath));

        return vaultMgr;
    }

    /**
     * Checks node status. If it's STOPPING then prevents further starting and throws NodeStoppingException
     * that will lead to stopping already started components later on,
     * otherwise starts component and add it to started components list.
     *
     * @param nodeName Node name.
     * @param startedComponents List of already started components for given node.
     * @param component Ignite component to start.
     * @param <T> Ignite component type.
     * @return Started ignite component.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    private static <T extends IgniteComponent> T doStartComponent(
        @NotNull String nodeName,
        @NotNull List<IgniteComponent> startedComponents,
        @NotNull T component
    ) throws NodeStoppingException {
        if (nodesStatus.get(nodeName) == NodeState.STOPPING)
            throw new NodeStoppingException("Node=[" + nodeName + "] was stopped.");
        else {
            startedComponents.add(component);

            component.start();

            return component;
        }
    }

    /**
     * Calls {@link IgniteComponent#beforeNodeStop()} and then {@link IgniteComponent#stop()} for all components
     * in start-reverse-order. Cleanups node started components map and node status map.
     *
     * @param nodeName Node name.
     * @param startedComponents List of already started components for given node.
     */
    private static void doStopNode(@NotNull String nodeName, @NotNull List<IgniteComponent> startedComponents) {
        ListIterator<IgniteComponent> beforeStopIter =
            startedComponents.listIterator(startedComponents.size() - 1);

        while (beforeStopIter.hasPrevious()) {
            IgniteComponent componentToExecuteBeforeNodeStop = beforeStopIter.previous();

            try {
                componentToExecuteBeforeNodeStop.beforeNodeStop();
            }
            catch (Exception e) {
                LOG.error("Unable to execute before node stop on the component=[" +
                    componentToExecuteBeforeNodeStop + "] within node=[" + nodeName + ']', e);
            }
        }

        ListIterator<IgniteComponent> stopIter =
            startedComponents.listIterator(startedComponents.size() - 1);

        while (stopIter.hasPrevious()) {
            IgniteComponent componentToStop = stopIter.previous();

            try {
                componentToStop.stop();
            }
            catch (Exception e) {
                LOG.error("Unable to stop component=[" + componentToStop + "] within node=[" + nodeName + ']', e);
            }
        }

        nodesStartedComponents.remove(nodeName);

        nodesStatus.remove(nodeName);
    }

    /** */
    private static void ackSuccessStart() {
        LOG.info("Apache Ignite started successfully!");
    }

    /** */
    private static void ackBanner() {
        String ver = IgniteProperties.get(VER_KEY);

        String banner = String.join("\n", BANNER);

        LOG.info(() ->
            LoggerMessageHelper.format("{}\n" + " ".repeat(22) + "Apache Ignite ver. {}\n", banner, ver),
            null);
    }

    /**
     * Node state.
     */
    private enum NodeState {
        /** */
        STARTING,

        /** */
        STARTED,

        /** */
        STOPPING
    }
}
