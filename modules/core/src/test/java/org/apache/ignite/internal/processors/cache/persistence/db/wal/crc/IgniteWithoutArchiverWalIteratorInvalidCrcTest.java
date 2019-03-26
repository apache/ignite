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

package org.apache.ignite.internal.processors.cache.persistence.db.wal.crc;

import java.io.File;
import java.util.List;
import java.util.Random;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;

/** */
public class IgniteWithoutArchiverWalIteratorInvalidCrcTest extends GridCommonAbstractTest {
    /** Size of inserting dummy value. */
    private static final int VALUE_SIZE = 4 * 1024;

    /** Size of WAL segment file. */
    private static final int WAL_SEGMENT_SIZE = 1024 * 1024;

    /** Count of WAL segment files in working directory. */
    private static final int WAL_SEGMENTS = DataStorageConfiguration.DFLT_WAL_SEGMENTS;

    /** Ignite instance. */
    protected IgniteEx ignite;

    /** Random instance for utility purposes. */
    protected Random random = new Random();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setWalSegmentSize(WAL_SEGMENT_SIZE)
                .setWalMode(WALMode.LOG_ONLY)
                .setWalArchivePath(DFLT_WAL_PATH) // disable archiving
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                )
        );

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        ignite = startGrid(0);

        ignite.cluster().active(true);

        IgniteCache<Integer, byte[]> cache = ignite.cache(DEFAULT_CACHE_NAME);

        byte[] val = new byte[VALUE_SIZE];

        // Fill value with random data.
        random.nextBytes(val);

        // Amount of values that's enough to fill working dir at least twice.
        int insertingCnt = 2 * WAL_SEGMENT_SIZE * WAL_SEGMENTS / VALUE_SIZE;
        for (int i = 0; i < insertingCnt; i++)
            cache.put(i, val);

        ignite.cluster().active(false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopGrid(0);

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void nodeShouldStartIfOldWalRecordCorrupted() throws Exception {
        stopGrid(0);

        IgniteWriteAheadLogManager walMgr = ignite.context().cache().context().wal();

        File walDir = U.field(walMgr, "walWorkDir");

        IgniteWalIteratorFactory iterFactory = new IgniteWalIteratorFactory();

        List<FileDescriptor> walFiles = getWalFiles(walDir, iterFactory);

        FileDescriptor penultimateWalFile = walFiles.get(walFiles.size() - 2);

        WalTestUtils.corruptWalSegmentFile(penultimateWalFile, iterFactory, random);

        startGrid(0);
    }

    /** */
    @Test
    public void nodeShouldStartIfTailRecordCorrupted() throws Exception {
        stopGrid(0);

        IgniteWriteAheadLogManager walMgr = ignite.context().cache().context().wal();

        File walDir = U.field(walMgr, "walWorkDir");

        IgniteWalIteratorFactory iterFactory = new IgniteWalIteratorFactory();

        List<FileDescriptor> walFiles = getWalFiles(walDir, iterFactory);

        Random corruptLastRecord = null;

        FileDescriptor lastWalFile = walFiles.get(walFiles.size() - 1);

        WalTestUtils.corruptWalSegmentFile(lastWalFile, iterFactory, corruptLastRecord);

        startGrid(0);
    }

     /**
     * @param walDir Wal directory.
     * @param iterFactory Iterator factory.
     * @return Last wal segment
     */
    private List<FileDescriptor> getWalFiles(File walDir, IgniteWalIteratorFactory iterFactory) {
        return iterFactory.resolveWalFiles(
            new IgniteWalIteratorFactory.IteratorParametersBuilder()
                .filesOrDirs(walDir)
        );
    }
}
