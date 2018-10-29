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

package org.apache.ignite.internal.processors.compress;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.OpenOption;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PageCompression;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.file.AlignedBuffersDirectFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.configuration.PageCompression.LZ4;
import static org.apache.ignite.configuration.PageCompression.SKIP_GARBAGE;
import static org.apache.ignite.configuration.PageCompression.ZSTD;
import static org.apache.ignite.internal.processors.compress.CompressionProcessor.LZ4_DEFAULT_LEVEL;
import static org.apache.ignite.internal.processors.compress.CompressionProcessor.LZ4_MAX_LEVEL;
import static org.apache.ignite.internal.processors.compress.CompressionProcessor.LZ4_MIN_LEVEL;
import static org.apache.ignite.internal.processors.compress.CompressionProcessor.ZSTD_MAX_LEVEL;
import static org.apache.ignite.internal.processors.compress.CompressionProcessor.ZSTD_MIN_LEVEL;
import static org.apache.ignite.internal.processors.compress.PageCompressionIntegrationTest.PunchFileIO.assertPunched;
import static org.apache.ignite.internal.processors.compress.PageCompressionIntegrationTest.PunchFileIO.resetPunchCount;

/**
 *
 */
public class PageCompressionIntegrationTest extends GridCommonAbstractTest {
    /** */
    PageCompression compression;

    /** */
    Integer compressionLevel;

    /** */
    FileIOFactory fileIOFactory;

    /** */
    int pageSize = 16 * 1024;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();
        CompressionProcessorImpl.testMode = true;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        CompressionProcessorImpl.testMode = false;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        compression = null;
        compressionLevel = null;
        fileIOFactory = null;
        cleanPersistenceDir();
        resetPunchCount();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteName) throws Exception {
        DataRegionConfiguration drCfg = new DataRegionConfiguration()
            .setPersistenceEnabled(true)
            .setPageCompression(compression)
            .setPageCompressionLevel(compressionLevel);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setPageSize(pageSize)
            .setCheckpointFrequency(750)
            .setDefaultDataRegionConfiguration(drCfg)
            .setFileIOFactory(new PunchFileIOFactory(fileIOFactory));

        return super.getConfiguration(igniteName).setDataStorageConfiguration(dsCfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageCompression_RandomAccessFileIO_Zstd_Max() throws Exception {
        fileIOFactory = new RandomAccessFileIOFactory();
        compression = ZSTD;
        compressionLevel = ZSTD_MAX_LEVEL;

        doTestPageCompression();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageCompression_RandomAccessFileIO_Zstd_Default() throws Exception {
        fileIOFactory = new RandomAccessFileIOFactory();
        compression = ZSTD;
        compressionLevel = null;

        doTestPageCompression();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageCompression_RandomAccessFileIO_Zstd_Min() throws Exception {
        fileIOFactory = new RandomAccessFileIOFactory();
        compression = ZSTD;
        compressionLevel = ZSTD_MIN_LEVEL;

        doTestPageCompression();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageCompression_RandomAccessFileIO_Lz4_Max() throws Exception {
        fileIOFactory = new RandomAccessFileIOFactory();
        compression = LZ4;
        compressionLevel = LZ4_MAX_LEVEL;

        doTestPageCompression();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageCompression_RandomAccessFileIO_Lz4_Default() throws Exception {
        fileIOFactory = new RandomAccessFileIOFactory();
        compression = LZ4;
        compressionLevel = null;

        doTestPageCompression();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageCompression_RandomAccessFileIO_Lz4_Min() throws Exception {
        assertEquals(LZ4_MIN_LEVEL, LZ4_DEFAULT_LEVEL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageCompression_RandomAccessFileIO_SkipGarbage() throws Exception {
        fileIOFactory = new RandomAccessFileIOFactory();
        compression = SKIP_GARBAGE;

        doTestPageCompression();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageCompression_AsyncFileIO_Zstd_Max() throws Exception {
        fileIOFactory = new AsyncFileIOFactory();
        compression = ZSTD;
        compressionLevel = ZSTD_MAX_LEVEL;

        doTestPageCompression();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageCompression_AsyncFileIO_Zstd_Default() throws Exception {
        fileIOFactory = new AsyncFileIOFactory();
        compression = ZSTD;
        compressionLevel = null;

        doTestPageCompression();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageCompression_AsyncFileIO_Zstd_Min() throws Exception {
        fileIOFactory = new AsyncFileIOFactory();
        compression = ZSTD;
        compressionLevel = ZSTD_MIN_LEVEL;

        doTestPageCompression();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageCompression_AsyncFileIO_Lz4_Max() throws Exception {
        fileIOFactory = new AsyncFileIOFactory();
        compression = LZ4;
        compressionLevel = LZ4_MAX_LEVEL;

        doTestPageCompression();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageCompression_AsyncFileIO_Lz4_Default() throws Exception {
        fileIOFactory = new AsyncFileIOFactory();
        compression = LZ4;
        compressionLevel = null;

        doTestPageCompression();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageCompression_AsyncFileIO_Lz4_Min() throws Exception {
        assertEquals(LZ4_MIN_LEVEL, LZ4_DEFAULT_LEVEL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageCompression_AsyncFileIO_SkipGarbage() throws Exception {
        if (!U.isLinux())
            return;

        fileIOFactory = new RandomAccessFileIOFactory();
        compression = SKIP_GARBAGE;

        doTestPageCompression();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageCompression_DirectFileIO_Zstd_Max() throws Exception {
        if (!U.isLinux())
            return;

        fileIOFactory = newAlignedBufferFactory();
        compression = ZSTD;
        compressionLevel = ZSTD_MAX_LEVEL;

        doTestPageCompression();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageCompression_DirectFileIO_Zstd_Default() throws Exception {
        if (!U.isLinux())
            return;

        fileIOFactory = newAlignedBufferFactory();
        compression = ZSTD;
        compressionLevel = null;

        doTestPageCompression();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageCompression_DirectFileIO_Zstd_Min() throws Exception {
        if (!U.isLinux())
            return;

        fileIOFactory = newAlignedBufferFactory();
        compression = ZSTD;
        compressionLevel = ZSTD_MIN_LEVEL;

        doTestPageCompression();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageCompression_DirectFileIO_Lz4_Max() throws Exception {
        if (!U.isLinux())
            return;

        fileIOFactory = newAlignedBufferFactory();
        compression = LZ4;
        compressionLevel = LZ4_MAX_LEVEL;

        doTestPageCompression();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageCompression_DirectFileIO_Lz4_Default() throws Exception {
        if (!U.isLinux())
            return;

        fileIOFactory = newAlignedBufferFactory();
        compression = LZ4;
        compressionLevel = null;

        doTestPageCompression();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageCompression_DirectFileIO_Lz4_Min() throws Exception {
        assertEquals(LZ4_MIN_LEVEL, LZ4_DEFAULT_LEVEL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageCompression_DirectFileIO_SkipGarbage() throws Exception {
        if (!U.isLinux())
            return;

        fileIOFactory = newAlignedBufferFactory();
        compression = SKIP_GARBAGE;

        doTestPageCompression();
    }

    private AlignedBuffersDirectFileIOFactory newAlignedBufferFactory() throws Exception {
        File storePath = new File(super.getConfiguration().getDataStorageConfiguration().getStoragePath());

        return new AlignedBuffersDirectFileIOFactory(log, storePath, pageSize, new RandomAccessFileIOFactory());
    }

    /**
     * @throws Exception If failed.
     */
    void doTestPageCompression() throws Exception {
        String igniteName = getTestIgniteInstanceName();
        IgniteEx ignite = startGrid(igniteName);

        ignite.cluster().active(true);

        CacheConfiguration<Integer,TestVal> ccfg = new CacheConfiguration<Integer,TestVal>()
            .setName("test")
            .setBackups(0)
            .setAtomicityMode(ATOMIC)
            .setIndexedTypes(Integer.class, TestVal.class);

        IgniteCache<Integer,TestVal> cache = ignite.getOrCreateCache(ccfg);

        int cnt = 2_000;

        for (int i = 0; i < cnt; i++)
            assertTrue(cache.putIfAbsent(i, new TestVal(i)));

        for (int i = 0; i < cnt; i += 2)
            assertEquals(new TestVal(i), cache.getAndRemove(i));

        U.sleep(1000);

        assertPunched(true);

//        stopGrid(igniteName, false, true);
//
//        ignite = startGrid(0);
//
//        resetPunchCount();
//
//        cache = ignite.getOrCreateCache(ccfg);
//
//        for (int i = 0; i < cnt; i++) {
//            if (i % 2 == 0)
//                assertNull(cache.get(i));
//            else
//                assertEquals(new TestVal(i), cache.get(i));
//        }
//
//        assertPunched(false);
//
//        cache.put(-1, new TestVal(-1));
//
//        U.sleep(1000);
//
//        assertPunched(true);
    }

    /**
     */
    static class TestVal implements Serializable {
        /** */
        static final long serialVersionUID = 1L;

        /** */
        @QuerySqlField
        String str;

        /** */
        int i;

        /** */
        @QuerySqlField
        long x;

        /** */
        @QuerySqlField
        UUID id;

        TestVal(int i) {
            this.str =  i + "bla bla bla!";
            this.i = -i;
            this.x = 0xffaabbccdd773311L + i;
            this.id = new UUID(i,-i);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestVal testVal = (TestVal)o;

            if (i != testVal.i) return false;
            if (x != testVal.x) return false;
            if (str != null ? !str.equals(testVal.str) : testVal.str != null) return false;
            return id != null ? id.equals(testVal.id) : testVal.id == null;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = str != null ? str.hashCode() : 0;
            result = 31 * result + i;
            result = 31 * result + (int)(x ^ (x >>> 32));
            result = 31 * result + (id != null ? id.hashCode() : 0);
            return result;
        }
    }

    /**
     */
    static class PunchFileIO extends FileIODecorator {
        /** */
        static final AtomicLong punchedBytes = new AtomicLong();

        /**
         * @param delegate File I/O delegate
         */
        public PunchFileIO(FileIO delegate) {
            super(Objects.requireNonNull(delegate));
        }

        /**
         */
        static void assertPunched(boolean punched) {
            assertEquals(punched, punchedBytes.getAndSet(0) > 0L);
        }

        /**
         */
        static void resetPunchCount() {
            punchedBytes.set(0L);
        }

        /** {@inheritDoc} */
        @Override public int getFileSystemBlockSize() {
            if (U.isLinux())
                return delegate.getFileSystemBlockSize();

            return 4 * 1024;
        }

        /** {@inheritDoc} */
        @Override public int punchHole(long pos, int len) {
            if (U.isLinux())
                len = delegate.punchHole(pos, len);
            else {
                int blockSize = getFileSystemBlockSize();
                len = len / blockSize * blockSize;
            }

            assertTrue(len >= 0);

            if (len != 0)
                punchedBytes.addAndGet(len);

            return len;
        }
    }

    /**
     *
     */
    static class PunchFileIOFactory implements FileIOFactory {
        /** */
        final FileIOFactory delegate;

        /**
         * @param delegate Delegate.
         */
        PunchFileIOFactory(FileIOFactory delegate) {
            this.delegate = Objects.requireNonNull(delegate);
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file) throws IOException {
            return new PunchFileIO(delegate.create(file));
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            return new PunchFileIO(delegate.create(file, modes));
        }
    }
}
