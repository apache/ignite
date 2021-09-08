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

package org.apache.ignite.internal.storage;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.schemas.runner.NodeConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.storage.Data;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.metastorage.MetaStorageManager.APPLIED_REV;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the {@link DistributedConfigurationStorage}.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ITDistributedConfigurationStorageTest {
    /**
     * An emulation of an Ignite node, that only contains components necessary for tests.
     */
    private static class Node {
        /** */
        private final NetworkAddress addr = new NetworkAddress("localhost", 10000);

        /** */
        private final VaultManager vaultManager;

        /** */
        private final ClusterService clusterService;

        /** */
        private final Loza raftManager;

        /** */
        private final ConfigurationManager cfgManager;

        /** */
        private final MetaStorageManager metaStorageManager;

        /** */
        private final DistributedConfigurationStorage cfgStorage;

        /**
         * Constructor that simply creates a subset of components of this node.
         */
        Node(Path workDir) {
            vaultManager = new VaultManager(new PersistentVaultService(workDir.resolve("vault")));

            clusterService = ClusterServiceTestUtils.clusterService(
                addr.toString(),
                addr.port(),
                new StaticNodeFinder(List.of(addr)),
                new MessageSerializationRegistryImpl(),
                new TestScaleCubeClusterServiceFactory()
            );

            raftManager = new Loza(clusterService, workDir);

            List<RootKey<?, ?>> rootKeys = List.of(NodeConfiguration.KEY);

            cfgManager = new ConfigurationManager(
                rootKeys,
                Map.of(),
                new LocalConfigurationStorage(vaultManager),
                List.of()
            );

            metaStorageManager = new MetaStorageManager(
                vaultManager,
                cfgManager,
                clusterService,
                raftManager,
                new SimpleInMemoryKeyValueStorage()
            );

            cfgStorage = new DistributedConfigurationStorage(metaStorageManager, vaultManager);
        }

        /**
         * Starts the created components.
         */
        void start() throws Exception {
            vaultManager.start();

            cfgManager.start();

            // metastorage configuration
            var config = String.format("{\"node\": {\"metastorageNodes\": [ \"%s\" ]}}", addr);

            cfgManager.bootstrap(config);

            Stream.of(clusterService, raftManager, metaStorageManager).forEach(IgniteComponent::start);

            // this is needed to avoid assertion errors
            cfgStorage.registerConfigurationListener(changedEntries -> completedFuture(null));

            // deploy watches to propagate data from the metastore into the vault
            metaStorageManager.deployWatches();
        }

        /**
         * Stops the created components.
         */
        void stop() throws Exception {
            var components =
                List.of(metaStorageManager, raftManager, clusterService, cfgManager, vaultManager);

            for (IgniteComponent igniteComponent : components)
                igniteComponent.beforeNodeStop();

            for (IgniteComponent component : components)
                component.stop();
        }
    }

    /**
     * Tests a scenario when a node is restarted with an existing PDS folder. A node is started and some data is written
     * to the distributed configuration storage. We then expect that the same data can be read by the node after
     * restart.
     *
     * @see <a href="https://issues.apache.org/jira/browse/IGNITE-15213">IGNITE-15213</a>
     */
    @Test
    void testRestartWithPds(@WorkDirectory Path workDir) throws Exception {
        var node = new Node(workDir);

        Map<String, Serializable> data = Map.of("foo", "bar");

        try {
            node.start();

            assertThat(node.cfgStorage.write(data, 0), willBe(equalTo(true)));

            waitForCondition(() -> Objects.nonNull(node.vaultManager.get(APPLIED_REV).join().value()), 3000);
        }
        finally {
            node.stop();
        }

        var node2 = new Node(workDir);

        try {
            node2.start();

            Data storageData = node2.cfgStorage.readAll();

            assertThat(storageData.values(), equalTo(data));
        }
        finally {
            node2.stop();
        }
    }
}
