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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.ignite.app.Ignite;
import org.apache.ignite.app.Ignition;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.schemas.network.NetworkConfiguration;
import org.apache.ignite.configuration.schemas.network.NetworkView;
import org.apache.ignite.configuration.schemas.runner.ClusterConfiguration;
import org.apache.ignite.configuration.schemas.runner.NodeConfiguration;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.affinity.AffinityManager;
import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.processors.query.calcite.SqlQueryProcessor;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.storage.DistributedConfigurationStorage;
import org.apache.ignite.internal.storage.LocalConfigurationStorage;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.VaultService;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.LoggerMessageHelper;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.scalecube.ScaleCubeClusterServiceFactory;
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

        ackBanner();

        VaultManager vaultMgr = createVault(nodeName, workDir);

        boolean cfgBootstrappedFromPds = vaultMgr.bootstrapped();

        List<RootKey<?, ?>> rootKeys = Arrays.asList(
            NetworkConfiguration.KEY,
            NodeConfiguration.KEY,
            ClusterConfiguration.KEY,
            TablesConfiguration.KEY
        );

        List<ConfigurationStorage> cfgStorages =
            new ArrayList<>(Collections.singletonList(new LocalConfigurationStorage(vaultMgr)));

        // Bootstrap local configuration manager.
        ConfigurationManager locConfigurationMgr = new ConfigurationManager(rootKeys, cfgStorages);

        if (!cfgBootstrappedFromPds && cfgContent != null)
            try {
                locConfigurationMgr.bootstrap(cfgContent, ConfigurationType.LOCAL);
            }
            catch (Exception e) {
                LOG.warn("Unable to parse user-specific configuration, default configuration will be used: {}", e.getMessage());
            }
        else if (cfgContent != null)
            LOG.warn("User specific configuration will be ignored, cause vault was bootstrapped with pds configuration");
        else
            locConfigurationMgr.configurationRegistry().startStorageConfigurations(ConfigurationType.LOCAL);

        NetworkView netConfigurationView =
            locConfigurationMgr.configurationRegistry().getConfiguration(NetworkConfiguration.KEY).value();

        var serializationRegistry = new MessageSerializationRegistryImpl();

        List<NetworkAddress> peers = Arrays.stream(netConfigurationView.netClusterNodes())
            .map(NetworkAddress::from)
            .collect(Collectors.toUnmodifiableList());

        // Network startup.
        ClusterService clusterNetSvc = new ScaleCubeClusterServiceFactory().createClusterService(
            new ClusterLocalConfiguration(
                nodeName,
                netConfigurationView.port(),
                peers,
                serializationRegistry
            )
        );

        clusterNetSvc.start();

        // Raft Component startup.
        Loza raftMgr = new Loza(clusterNetSvc);

        // Meta storage Component startup.
        MetaStorageManager metaStorageMgr = new MetaStorageManager(
            vaultMgr,
            locConfigurationMgr,
            clusterNetSvc,
            raftMgr
        );

        // TODO IGNITE-14578 Bootstrap configuration manager with distributed configuration.
        cfgStorages.add(new DistributedConfigurationStorage(metaStorageMgr, vaultMgr));

        // Start configuration manager.
        ConfigurationManager configurationMgr = new ConfigurationManager(rootKeys, cfgStorages);

        // Baseline manager startup.
        BaselineManager baselineMgr = new BaselineManager(configurationMgr, metaStorageMgr, clusterNetSvc);

        // Affinity manager startup.
        AffinityManager affinityMgr = new AffinityManager(configurationMgr, metaStorageMgr, baselineMgr);

        SchemaManager schemaMgr = new SchemaManager(configurationMgr, metaStorageMgr, vaultMgr);

        // Distributed table manager startup.
        TableManager distributedTblMgr = new TableManager(
            configurationMgr,
            metaStorageMgr,
            schemaMgr,
            affinityMgr,
            raftMgr,
            vaultMgr
        );

        SqlQueryProcessor qryProc = new SqlQueryProcessor(
            clusterNetSvc,
            distributedTblMgr
        );

        // TODO IGNITE-14579 Start rest manager.

        // Deploy all resisted watches cause all components are ready and have registered their listeners.
        metaStorageMgr.deployWatches();

        ackSuccessStart();

        return new IgniteImpl(distributedTblMgr, vaultMgr, qryProc);
    }

    /**
     * Starts the Vault component.
     */
    private static VaultManager createVault(String nodeName, Path workDir) {
        Path vaultPath = workDir.resolve(VAULT_DB_PATH);

        try {
            Files.createDirectories(vaultPath);
        }
        catch (IOException e) {
            throw new IgniteInternalException(e);
        }

        var vaultMgr = new VaultManager(new PersistentVaultService(vaultPath));

        vaultMgr.putName(nodeName).join();

        return vaultMgr;
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
}
