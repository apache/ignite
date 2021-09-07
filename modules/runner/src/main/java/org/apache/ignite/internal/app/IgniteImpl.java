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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.app.IgnitionManager;
import org.apache.ignite.client.handler.ClientHandlerModule;
import org.apache.ignite.configuration.schemas.clientconnector.ClientConnectorConfiguration;
import org.apache.ignite.configuration.schemas.network.NetworkConfiguration;
import org.apache.ignite.configuration.schemas.rest.RestConfiguration;
import org.apache.ignite.configuration.schemas.runner.ClusterConfiguration;
import org.apache.ignite.configuration.schemas.runner.NodeConfiguration;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.affinity.AffinityManager;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.processors.query.calcite.SqlQueryProcessor;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.storage.LocalConfigurationStorage;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.network.scalecube.ScaleCubeClusterServiceFactory;
import org.apache.ignite.rest.RestModule;
import org.apache.ignite.table.manager.IgniteTables;
import org.apache.ignite.tx.IgniteTransactions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite internal implementation.
 */
public class IgniteImpl implements Ignite {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(IgniteImpl.class);

    /**
     * Path to the persistent storage used by the {@link org.apache.ignite.internal.vault.VaultService} component.
     */
    private static final Path VAULT_DB_PATH = Paths.get("vault");

    /**
     * Path for the partitions persistent storage.
     */
    private static final Path PARTITIONS_STORE_PATH = Paths.get("db");

    /** Ignite node name. */
    private final String name;

    /** Vault manager. */
    private final VaultManager vaultMgr;

    /** Configuration manager that handles node (local) configuration. */
    private final ConfigurationManager nodeCfgMgr;

    /** Cluster service (cluster network manager). */
    private final ClusterService clusterSvc;

    /** Raft manager. */
    private final Loza raftMgr;

    /** Meta storage manager. */
    private final MetaStorageManager metaStorageMgr;

    /** Configuration manager that handles cluster (distributed) configuration. */
    private final ConfigurationManager clusterCfgMgr;

    /** Baseline manager. */
    private final BaselineManager baselineMgr;

    /** Affinity manager. */
    private final AffinityManager affinityMgr;

    /** Schema manager. */
    private final SchemaManager schemaMgr;

    /** Distributed table manager. */
    private final TableManager distributedTblMgr;

    /** Query engine. */
    private final SqlQueryProcessor qryEngine;

    /** Rest module. */
    private final RestModule restModule;

    /** Client handler module. */
    private final ClientHandlerModule clientHandlerModule;

    /** Node status. Adds ability to stop currently starting node. */
    private final AtomicReference<Status> status = new AtomicReference<>(Status.STARTING);

    /**
     * The Constructor.
     *
     * @param name Ignite node name.
     * @param workDir Work directory for the started node. Must not be {@code null}.
     */
    IgniteImpl(
        String name,
        Path workDir
    ) {
        this.name = name;

        vaultMgr = createVault(workDir);

        nodeCfgMgr = new ConfigurationManager(
            Arrays.asList(
                NetworkConfiguration.KEY,
                NodeConfiguration.KEY,
                RestConfiguration.KEY,
                ClientConnectorConfiguration.KEY
            ),
            Map.of(),
            new LocalConfigurationStorage(vaultMgr),
            List.of()
        );

        clusterSvc = new ScaleCubeClusterServiceFactory().createClusterService(
            new ClusterLocalConfiguration(
                name,
                new MessageSerializationRegistryImpl()
            ),
            nodeCfgMgr,
            () -> StaticNodeFinder.fromConfiguration(nodeCfgMgr.configurationRegistry().
                getConfiguration(NetworkConfiguration.KEY).value())
        );

        raftMgr = new Loza(clusterSvc, workDir);

        metaStorageMgr = new MetaStorageManager(
            vaultMgr,
            nodeCfgMgr,
            clusterSvc,
            raftMgr
        );

        clusterCfgMgr = new ConfigurationManager(
            Arrays.asList(
                ClusterConfiguration.KEY,
                TablesConfiguration.KEY
            ),
            Map.of(),
            new DistributedConfigurationStorage(metaStorageMgr, vaultMgr),
            List.of()
        );

        baselineMgr = new BaselineManager(
            clusterCfgMgr,
            metaStorageMgr,
            clusterSvc
        );

        affinityMgr = new AffinityManager(
            clusterCfgMgr,
            metaStorageMgr,
            baselineMgr
        );

        schemaMgr = new SchemaManager(
            clusterCfgMgr,
            metaStorageMgr,
            vaultMgr
        );

        distributedTblMgr = new TableManager(
            nodeCfgMgr,
            clusterCfgMgr,
            metaStorageMgr,
            schemaMgr,
            affinityMgr,
            raftMgr,
            getPartitionsStorePath(workDir)
        );

        qryEngine = new SqlQueryProcessor(
            clusterSvc,
            distributedTblMgr
        );

        restModule = new RestModule(nodeCfgMgr, clusterCfgMgr);

        clientHandlerModule = new ClientHandlerModule(distributedTblMgr, nodeCfgMgr.configurationRegistry());
    }

    /**
     * Starts ignite node.
     *
     * @param cfg Optional node configuration based on {@link org.apache.ignite.configuration.schemas.runner.NodeConfigurationSchema}
     * and {@link org.apache.ignite.configuration.schemas.network.NetworkConfigurationSchema}. Following rules are used
     * for applying the configuration properties:
     * <ol>
     * <li>Specified property overrides existing one or just applies itself if it wasn't
     * previously specified.</li>
     * <li>All non-specified properties either use previous value or use default one from
     * corresponding configuration schema.</li>
     * </ol>
     * So that, in case of initial node start (first start ever) specified configuration, supplemented with defaults, is
     * used. If no configuration was provided defaults are used for all configuration properties. In case of node
     * restart, specified properties override existing ones, non specified properties that also weren't specified
     * previously use default values. Please pay attention that previously specified properties are searched in the
     * {@code workDir} specified by the user.
     */
    public void start(@Nullable String cfg) {
        List<IgniteComponent> startedComponents = new ArrayList<>();

        try {
            // Vault startup.
            doStartComponent(
                name,
                startedComponents,
                vaultMgr
            );

            vaultMgr.putName(name).join();

            // Node configuration manager startup.
            doStartComponent(
                name,
                startedComponents,
                nodeCfgMgr);

            // Node configuration manager bootstrap.
            if (cfg != null) {
                try {
                    nodeCfgMgr.bootstrap(cfg);
                }
                catch (Exception e) {
                    LOG.warn("Unable to parse user-specific configuration, default configuration will be used: {}",
                        e.getMessage());
                }
            }
            else
                nodeCfgMgr.configurationRegistry().initializeDefaults();

            // Start the remaining components.
            List<IgniteComponent> otherComponents = List.of(
                clusterSvc,
                raftMgr,
                metaStorageMgr,
                clusterCfgMgr,
                baselineMgr,
                affinityMgr,
                schemaMgr,
                distributedTblMgr,
                qryEngine,
                restModule,
                clientHandlerModule
            );

            for (IgniteComponent component : otherComponents)
                doStartComponent(name, startedComponents, component);

            // Deploy all registered watches because all components are ready and have registered their listeners.
            metaStorageMgr.deployWatches();

            if (!status.compareAndSet(Status.STARTING, Status.STARTED))
                throw new NodeStoppingException();
        }
        catch (Exception e) {
            String errMsg = "Unable to start node=[" + name + "].";

            LOG.error(errMsg, e);

            doStopNode(startedComponents);

            throw new IgniteException(errMsg, e);
        }
    }

    /**
     * Stops ignite node.
     */
    public void stop() {
        AtomicBoolean explicitStop = new AtomicBoolean();

        status.getAndUpdate(status -> {
            if (status == Status.STARTED)
                explicitStop.set(true);
            else
                explicitStop.set(false);

            return Status.STOPPING;
        });

        if (explicitStop.get()) {
            doStopNode(List.of(vaultMgr, nodeCfgMgr, clusterSvc, raftMgr, metaStorageMgr,
                clusterCfgMgr, baselineMgr, affinityMgr, schemaMgr, distributedTblMgr, qryEngine, restModule, clientHandlerModule));
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteTables tables() {
        return distributedTblMgr;
    }

    public SqlQueryProcessor queryEngine() {
        return qryEngine;
    }

    /** {@inheritDoc} */
    @Override public IgniteTransactions transactions() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        IgnitionManager.stop(name);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /**
     * @return Node configuration.
     */
    public ConfigurationRegistry nodeConfiguration() {
        return nodeCfgMgr.configurationRegistry();
    }

    /**
     * @return Cluster configuration.
     */
    public ConfigurationRegistry clusterConfiguration() {
        return clusterCfgMgr.configurationRegistry();
    }

    /**
     * @return Client handler module.
     */
    public ClientHandlerModule clientHandlerModule() {
        return clientHandlerModule;
    }

    /**
     * Checks node status. If it's {@link Status#STOPPING} then prevents further starting and throws NodeStoppingException that will
     * lead to stopping already started components later on, otherwise starts component and add it to started components
     * list.
     *
     * @param nodeName Node name.
     * @param startedComponents List of already started components for given node.
     * @param component Ignite component to start.
     * @param <T> Ignite component type.
     * @throws NodeStoppingException If node stopping intention was detected.
     */
    private <T extends IgniteComponent> void doStartComponent(
        @NotNull String nodeName,
        @NotNull List<IgniteComponent> startedComponents,
        @NotNull T component
    ) throws NodeStoppingException {
        if (status.get() == Status.STOPPING)
            throw new NodeStoppingException("Node=[" + nodeName + "] was stopped.");
        else {
            startedComponents.add(component);

            component.start();
        }
    }

    /**
     * Calls {@link IgniteComponent#beforeNodeStop()} and then {@link IgniteComponent#stop()} for all components in
     * start-reverse-order. Cleanups node started components map and node status map.
     *
     * @param startedComponents List of already started components for given node.
     */
    private void doStopNode(@NotNull List<IgniteComponent> startedComponents) {
        ListIterator<IgniteComponent> beforeStopIter =
            startedComponents.listIterator(startedComponents.size());

        while (beforeStopIter.hasPrevious()) {
            IgniteComponent componentToExecBeforeNodeStop = beforeStopIter.previous();

            try {
                componentToExecBeforeNodeStop.beforeNodeStop();
            }
            catch (Exception e) {
                LOG.error("Unable to execute before node stop on the component=[" +
                    componentToExecBeforeNodeStop + "] within node=[" + name + ']', e);
            }
        }

        ListIterator<IgniteComponent> stopIter =
            startedComponents.listIterator(startedComponents.size());

        while (stopIter.hasPrevious()) {
            IgniteComponent componentToStop = stopIter.previous();

            try {
                componentToStop.stop();
            }
            catch (Exception e) {
                LOG.error("Unable to stop component=[" + componentToStop + "] within node=[" + name + ']', e);
            }
        }
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

        return new VaultManager(new PersistentVaultService(vaultPath));
    }

    /**
     * Returns a path to the partitions store directory. Creates a directory if it doesn't exist.
     *
     * @param workDir Ignite work directory.
     * @return Partitions store path.
     */
    @NotNull
    private static Path getPartitionsStorePath(Path workDir) {
        Path partitionsStore = workDir.resolve(PARTITIONS_STORE_PATH);

        try {
            Files.createDirectories(partitionsStore);
        }
        catch (IOException e) {
            throw new IgniteInternalException("Failed to create directory for partitions storage: " + e.getMessage(), e);
        }

        return partitionsStore;
    }

    /**
     * Node state.
     */
    private enum Status {
        /** */
        STARTING,

        /** */
        STARTED,

        /** */
        STOPPING
    }
}
