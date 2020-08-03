/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

/**
 *
 */
public class IgnitePdsNoSpaceLeftOnDeviceTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        final DataStorageConfiguration dataStorageConfiguration = new DataStorageConfiguration();

        dataStorageConfiguration.getDefaultDataRegionConfiguration().setPersistenceEnabled(true).setMaxSize(1 << 24);
        dataStorageConfiguration.setFileIOFactory(new FailingFileIOFactory());

        cfg.setDataStorageConfiguration(dataStorageConfiguration);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setName(DEFAULT_CACHE_NAME);
        ccfg.setBackups(1);

        cfg.setCacheConfiguration(ccfg);

        cfg.setFailureHandler(new StopNodeFailureHandler());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * The tests to validate <a href="https://issues.apache.org/jira/browse/IGNITE-9120">IGNITE-9120</a>
     * Metadata writer does not propagate error to failure handler.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testWhileWritingBinaryMetadataFile() throws Exception {
        final IgniteEx ignite0 = startGrid(0);

        final IgniteEx ignite1 = startGrid(1);

        FailingFileIOFactory.setUnluckyConsistentId(ignite1.localNode().consistentId().toString());

        ignite0.cluster().active(true);

        final IgniteCache cache = ignite0.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 30; i++) {
            try (Transaction tx = ignite0.transactions().txStart()) {
                cache.put(1, ignite0.binary().builder("test").setField("field1", i).build());

                cache.put(1 << i, ignite0.binary().builder("test").setField("field1", i).build());

                tx.commit();
            }
            catch (Exception e) {
            }
        }

        waitForTopology(1);
    }

    /**
     * Generating error "No space left on device" when writing binary_meta file on the second node
     */
    private static class FailingFileIOFactory implements FileIOFactory {
        /**
         *
         */
        private static final long serialVersionUID = 0L;

        /**
         *
         */
        private final FileIOFactory delegateFactory = new RandomAccessFileIOFactory();

        /**
         * Node ConsistentId for which the error will be generated
         */
        private static final AtomicReference<String> unluckyConsistentId = new AtomicReference<>();

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            if (unluckyConsistentId.get() != null
                && file.getAbsolutePath().contains(unluckyConsistentId.get())
                && file.getAbsolutePath().contains(DataStorageConfiguration.DFLT_BINARY_METADATA_PATH))
                throw new IOException("No space left on device");

            return delegateFactory.create(file, modes);
        }

        /**
         * Set node ConsistentId for which the error will be generated
         *
         * @param unluckyConsistentId Node ConsistentId.
         */
        public static void setUnluckyConsistentId(String unluckyConsistentId) {
            FailingFileIOFactory.unluckyConsistentId.set(unluckyConsistentId);
        }
    }
}
