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

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.configuration.DiskPageCompression.LZ4;
import static org.apache.ignite.configuration.DiskPageCompression.SKIP_GARBAGE;
import static org.apache.ignite.configuration.DiskPageCompression.SNAPPY;
import static org.apache.ignite.configuration.DiskPageCompression.ZSTD;
import static org.apache.ignite.internal.processors.compress.CompressionProcessor.LZ4_DEFAULT_LEVEL;
import static org.apache.ignite.internal.processors.compress.CompressionProcessor.LZ4_MAX_LEVEL;
import static org.apache.ignite.internal.processors.compress.CompressionProcessor.LZ4_MIN_LEVEL;
import static org.apache.ignite.internal.processors.compress.CompressionProcessor.ZSTD_MAX_LEVEL;
import static org.apache.ignite.internal.processors.compress.CompressionProcessor.ZSTD_MIN_LEVEL;

/**
 *
 */
public abstract class AbstractPageCompressionIntegrationTest extends GridCommonAbstractTest {
    /** */
    protected DiskPageCompression compression;

    /** */
    protected Integer compressionLevel;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        compression = DiskPageCompression.DISABLED;
        compressionLevel = null;

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPageCompression_Zstd_Max() throws Exception {
        compression = ZSTD;
        compressionLevel = ZSTD_MAX_LEVEL;

        doTestPageCompression();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPageCompression_Zstd_Default() throws Exception {
        compression = ZSTD;
        compressionLevel = null;

        doTestPageCompression();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPageCompression_Zstd_Min() throws Exception {
        compression = ZSTD;
        compressionLevel = ZSTD_MIN_LEVEL;

        doTestPageCompression();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPageCompression_Lz4_Max() throws Exception {
        compression = LZ4;
        compressionLevel = LZ4_MAX_LEVEL;

        doTestPageCompression();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPageCompression_Lz4_Default() throws Exception {
        compression = LZ4;
        compressionLevel = null;

        doTestPageCompression();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPageCompression_Lz4_Min() throws Exception {
        assertEquals(LZ4_MIN_LEVEL, LZ4_DEFAULT_LEVEL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPageCompression_SkipGarbage() throws Exception {
        compression = SKIP_GARBAGE;

        doTestPageCompression();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPageCompression_Snappy() throws Exception {
        compression = SNAPPY;

        doTestPageCompression();
    }

    /**
     * @throws Exception If failed.
     */
    protected abstract void doTestPageCompression() throws Exception;

    /**
     *
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
            this.str = i + "bla bla bla!";
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
}
