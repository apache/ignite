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
 *
 */
package org.apache.ignite.compatibility.persistence;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Tests migration of metastorage.
 */
public class MetaStorageCompatibilityTest extends IgnitePersistenceCompatibilityAbstractTest {
    /** Consistent id. */
    private static final String CONSISTENT_ID_1 = "node1";

    /** Consistent id. */
    private static final String CONSISTENT_ID_2 = "node2";

    /** Ignite version. */
    private static final String IGNITE_VERSION = "2.4.0";

    /** Filename of index. */
    private static final String INDEX_BIN_FILE = "index.bin";

    /** Filename of partition. */
    private static final String PART_FILE = "part-0.bin";

    /**
     * The zero partition hasn't full baseline information. The last changes are read from WAL.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMigrationWithoutFullBaselineIntoPartition() throws Exception {
        try {
            U.delete(new File(U.defaultWorkDirectory()));

            startGrid(1, IGNITE_VERSION, new ConfigurationClosure(CONSISTENT_ID_1), new ActivateAndForceCheckpointClosure());

            startGrid(2, IGNITE_VERSION, new ConfigurationClosure(CONSISTENT_ID_2), new NewBaselineTopologyClosure());

            stopAllGrids();

            try (Ignite ig0 = IgnitionEx.start(prepareConfig(getConfiguration(), CONSISTENT_ID_1))) {
                try (Ignite ig1 = IgnitionEx.start(prepareConfig(getConfiguration(), CONSISTENT_ID_2))) {
                    // No-op.
                }
            }

            assertFalse(metastorageFileExists(INDEX_BIN_FILE));
            assertFalse(metastorageFileExists(PART_FILE));

            try (Ignite ig0 = IgnitionEx.start(prepareConfig(getConfiguration(), CONSISTENT_ID_1))) {
                try (Ignite ig1 = IgnitionEx.start(prepareConfig(getConfiguration(), CONSISTENT_ID_2))) {
                    assertTrue(GridTestUtils.waitForCondition(() -> ig1.cluster().active(), 10_000));
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Tests that BLT can be changed and persisted after metastorage migration.
     */
    @Test
    public void testMigrationToNewBaselineSetNewBaselineAfterMigration() throws Exception {
        try {
            U.delete(new File(U.defaultWorkDirectory()));

            startGrid(1, IGNITE_VERSION, new ConfigurationClosure(CONSISTENT_ID_1), new ActivateAndStopClosure());

            stopAllGrids();

            try (Ignite ig0 = IgnitionEx.start(prepareConfig(getConfiguration(), CONSISTENT_ID_1))) {
                try (Ignite ig1 = IgnitionEx.start(prepareConfig(getConfiguration(), CONSISTENT_ID_2))) {
                    ig0.cluster().setBaselineTopology(ig1.cluster().topologyVersion());
                }
            }

            assertFalse(metastorageFileExists(INDEX_BIN_FILE));
            assertFalse(metastorageFileExists(PART_FILE));

            try (Ignite ig0 = IgnitionEx.start(prepareConfig(getConfiguration(), CONSISTENT_ID_1))) {
                try (Ignite ig1 = IgnitionEx.start(prepareConfig(getConfiguration(), CONSISTENT_ID_2))) {
                    assertTrue(GridTestUtils.waitForCondition(() -> ig1.cluster().active(), 10_000));
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    @Test
    public void testMigrationWithExceptionDuringTheProcess() throws Exception {
        try {
            U.delete(new File(U.defaultWorkDirectory()));

            startGrid(1, IGNITE_VERSION, new ConfigurationClosure(CONSISTENT_ID_1), new ActivateAndStopClosure());

            stopAllGrids();

            try (Ignite ig0 = IgnitionEx.start(prepareConfig(getConfiguration(), CONSISTENT_ID_1, PART_FILE))) {
                ig0.getOrCreateCache("default-cache").put(1, 1); // trigger checkpoint on close()
            }

            assertTrue(metastorageFileExists(INDEX_BIN_FILE));
            assertTrue(metastorageFileExists(PART_FILE));

            try (Ignite ig0 = IgnitionEx.start(prepareConfig(getConfiguration(), CONSISTENT_ID_1, INDEX_BIN_FILE))) {
                ig0.getOrCreateCache("default-cache").put(1, 1); // trigger checkpoint on close()
            }

            assertTrue(metastorageFileExists(INDEX_BIN_FILE));
            assertFalse(metastorageFileExists(PART_FILE));

            try (Ignite ig0 = IgnitionEx.start(prepareConfig(getConfiguration(), CONSISTENT_ID_1))) {
                try (Ignite ig1 = IgnitionEx.start(prepareConfig(getConfiguration(), CONSISTENT_ID_2))) {
                    ig0.cluster().setBaselineTopology(ig1.cluster().topologyVersion());
                }
            }

            try (Ignite ig0 = IgnitionEx.start(prepareConfig(getConfiguration(), CONSISTENT_ID_1))) {
                try (Ignite ig1 = IgnitionEx.start(prepareConfig(getConfiguration(), CONSISTENT_ID_2))) {
                    assertTrue(GridTestUtils.waitForCondition(() -> ig1.cluster().active(), 10_000));
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Checks that file exists.
     *
     * @param fileName File name.
     */
    private boolean metastorageFileExists(String fileName) throws IgniteCheckedException {
        return new File(U.defaultWorkDirectory() + "/db/" + U.maskForFileName(CONSISTENT_ID_1) + "/metastorage/" + fileName).exists();
    }

    /**
     * Updates the given ignite configuration.
     *
     * @param cfg              Ignite configuration to be updated.
     * @param consistentId     Consistent id.
     * @return Updated configuration.
     */
    private static IgniteConfiguration prepareConfig(IgniteConfiguration cfg, @Nullable String consistentId) {
        return prepareConfig(cfg, consistentId, null);
    }

    /**
     * Updates the given ignite configuration.
     *
     * @param cfg Ignite configuration to be updated.
     * @param consistentId Consistent id.
     * @param failOnFileRmv Indicates that an exception should be trown when partition/index file is going to be removed.
     * @return Updated configuration.
     */
    private static IgniteConfiguration prepareConfig(
        IgniteConfiguration cfg,
        @Nullable String consistentId,
        @Nullable String failOnFileRmv
    ) {
        cfg.setLocalHost("127.0.0.1");

        TcpDiscoverySpi disco = new TcpDiscoverySpi();
        disco.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setPeerClassLoadingEnabled(false);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)
                    .setInitialSize(10L * 1024 * 1024)
                    .setMaxSize(10L * 1024 * 1024))
            .setPageSize(4096);

        cfg.setDataStorageConfiguration(memCfg);

        if (consistentId != null) {
            cfg.setIgniteInstanceName(consistentId);
            cfg.setConsistentId(consistentId);
        }

        if (failOnFileRmv != null)
            cfg.getDataStorageConfiguration().setFileIOFactory(new FailingFileIOFactory(failOnFileRmv));

        return cfg;
    }

    /** */
    private static class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** Consistent id. */
        private final String consistentId;

        /**
         * Creates a new instance of Configuration closure.
         *
         * @param consistentId Consistent id.
         */
        public ConfigurationClosure(String consistentId) {
            this.consistentId = consistentId;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            prepareConfig(cfg, consistentId, null);
        }
    }

    /**
     * Post-startup close that activate the grid and force checkpoint.
     */
    private static class ActivateAndForceCheckpointClosure implements IgniteInClosure<Ignite> {
        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            try {
                ignite.active(true);

                ((IgniteEx)ignite).context().cache().context().database()
                    .wakeupForCheckpoint("force test checkpoint").get();

                ((GridCacheDatabaseSharedManager)(((IgniteEx)ignite).context().cache().context().database()))
                    .enableCheckpoints(false);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }

    /**
     * Activates the cluster and stops it after that.
     */
    private static class ActivateAndStopClosure implements IgniteInClosure<Ignite> {
        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            ignite.active(true);

            ignite.close();
        }
    }

    /**
     * Disables checkpointer and sets a new baseline topology.
     */
    private static class NewBaselineTopologyClosure implements IgniteInClosure<Ignite> {
        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            ((GridCacheDatabaseSharedManager)(((IgniteEx)ignite).context().cache().context().database()))
                .enableCheckpoints(false);

            long newTopVer = ignite.cluster().topologyVersion();

            ignite.cluster().setBaselineTopology(newTopVer);
        }
    }

    /**
     * Create File I/O which fails after second attempt to write to File
     */
    private static class FailingFileIOFactory implements FileIOFactory {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Indicates that an exception should be trown when this file is going to be removed. */
        private final String failOnFileRmv;

        /** */
        private final FileIOFactory delegateFactory = new RandomAccessFileIOFactory();

        /**
         * Creates a new instance of IO factory.
         */
        public FailingFileIOFactory() {
            failOnFileRmv = null;
        }

        /**
         * Creates a new instance of IO factory.
         *
         * @param failOnFileRmv Indicates that an exception should be trown when the given file is removed.
         */
        public FailingFileIOFactory(String failOnFileRmv) {
            this.failOnFileRmv = failOnFileRmv;
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            final FileIO delegate = delegateFactory.create(file, modes);

            return new FileIODecorator(delegate) {
                @Override public void clear() throws IOException {
                    if (failOnFileRmv != null && failOnFileRmv.equals(file.getName()))
                        throw new IOException("Test remove fail!");

                    super.clear();
                }
            };
        }
    }
}
