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
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.configuration.PageCompression.ZSTD;
import static org.apache.ignite.internal.processors.compress.PageCompressionIntegrationTest.PunchFileIO.assertPunched;

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

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
        PunchFileIO.resetPunchCount();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg.setPageSize(16 * 1024);
        dsCfg.setDefaultDataRegionConfiguration(
            new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setPageCompression(compression)
                .setPageCompressionLevel(compressionLevel)
        );
        dsCfg.setFileIOFactory(new PunchFileIOFactory(fileIOFactory));

        return super.getConfiguration().setDataStorageConfiguration(dsCfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPageCompression_RandomAccessFileIO_Zstd_Max() throws Exception {
        fileIOFactory = new RandomAccessFileIOFactory();
        compression = ZSTD;
        compressionLevel = 22;

        doTestPageCompression();
    }

    /**
     * @throws Exception If failed.
     */
    void doTestPageCompression() throws Exception {
        IgniteEx ignite = startGrid(0);

        CacheConfiguration<Integer,TestVal> ccfg = new CacheConfiguration<Integer,TestVal>()
            .setName("test")
            .setBackups(0)
            .setAtomicityMode(ATOMIC)
            .setIndexedTypes(Integer.TYPE, TestVal.class);

        IgniteCache<Integer,TestVal> cache = ignite.getOrCreateCache(ccfg);

        for (int i = 0; i < 20_000; i++)
            assertTrue(cache.putIfAbsent(i, new TestVal(i)));

        stopAllGrids(false);

        assertPunched(true);

        ignite = startGrid(0);

        cache = ignite.getOrCreateCache(ccfg);

        for (int i = 0; i < 20_000; i++)
            assertEquals(new TestVal(i), cache.get(i));

        assertPunched(false);
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
            return !U.isLinux() ? 1024 :
                super.getFileSystemBlockSize();
        }

        /** {@inheritDoc} */
        @Override public int punchHole(long pos, int len) {
            if (U.isLinux())
                len = super.punchHole(pos, len);
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
