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

package org.apache.ignite.cdc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory.IteratorParametersBuilder;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cdc.CdcSelfTest.WAL_ARCHIVE_TIMEOUT;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Check only {@link DataRecord} written to the WAL for in-memory cache. */
@RunWith(Parameterized.class)
public class WalForInMemoryTest extends GridCommonAbstractTest {
    /** */
    private static final int RECORD_COUNT = 10;

    /** */
    @Parameterized.Parameter
    public CacheMode mode;

    /** */
    @Parameterized.Parameter(1)
    public CacheAtomicityMode atomicityMode;

    /** */
    @Parameterized.Parameters(name = "mode={0}, atomicityMode={1}")
    public static Collection<?> parameters() {
        List<Object[]> params = new ArrayList<>();

        for (CacheMode mode : Arrays.asList(REPLICATED, PARTITIONED))
            for (CacheAtomicityMode atomicityMode : Arrays.asList(ATOMIC, TRANSACTIONAL))
                params.add(new Object[] {mode, atomicityMode});

        return params;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalMode(WALMode.FSYNC)
            .setWalForceArchiveTimeout(WAL_ARCHIVE_TIMEOUT)
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(false)
                .setCdcEnabled(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testOnlyDataRecordWritten() throws Exception {
        IgniteEx ignite = startGrid(0);

        IgniteCache<Integer, Integer> cache = ignite.createCache(
            new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
                .setCacheMode(mode)
                .setAtomicityMode(atomicityMode));

        long archiveIdx = ignite.context().cache().context().cdcWal().lastArchivedSegment();

        for (int i = 0; i < RECORD_COUNT; i++)
            cache.put(i, i);

        assertTrue(waitForCondition(
            () -> archiveIdx < ignite.context().cache().context().cdcWal().lastArchivedSegment(),
            getTestTimeout()
        ));

        String archive = U.resolveWorkDirectory(
            U.defaultWorkDirectory(),
            ignite.configuration().getDataStorageConfiguration().getWalArchivePath(),
            false
        ).getAbsolutePath();

        WALIterator iter = new IgniteWalIteratorFactory(log).iterator(new IteratorParametersBuilder()
            .ioFactory(new RandomAccessFileIOFactory())
            .filesOrDirs(archive));

        int walRecCnt = 0;

        while (iter.hasNext()) {
            IgniteBiTuple<WALPointer, WALRecord> rec = iter.next();

            assertTrue(rec.get2() instanceof DataRecord);

            DataRecord dataRec = (DataRecord)rec.get2();

            for (int i = 0; i < dataRec.entryCount(); i++)
                assertEquals(CU.cacheId(DEFAULT_CACHE_NAME), dataRec.get(i).cacheId());

            walRecCnt++;
        }

        assertEquals(RECORD_COUNT, walRecCnt);
    }
}
