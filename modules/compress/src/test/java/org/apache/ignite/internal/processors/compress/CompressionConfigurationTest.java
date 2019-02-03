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

import java.util.Objects;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.configuration.DiskPageCompression.LZ4;
import static org.apache.ignite.configuration.DiskPageCompression.ZSTD;

/**
 */
public class CompressionConfigurationTest extends GridCommonAbstractTest {
    /** */
    DiskPageCompression compression1;

    /** */
    DiskPageCompression compression2;

    /** */
    Integer level1;

    /** */
    Integer level2;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        CacheConfiguration<Object,Object> ccfg1 = new CacheConfiguration<>("cache1");

        ccfg1.setDiskPageCompression(compression1);
        ccfg1.setDiskPageCompressionLevel(level1);
        ccfg1.setGroupName("myGroup");

        CacheConfiguration<Object,Object> ccfg2 = new CacheConfiguration<>("cache2");

        ccfg2.setDiskPageCompression(compression2);
        ccfg2.setDiskPageCompressionLevel(level2);
        ccfg2.setGroupName("myGroup");

        return super.getConfiguration(instanceName).setCacheConfiguration(ccfg1, ccfg2);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInconsistentCacheGroupCompressionConfig() throws Exception {
        doTestConfig(ZSTD, null, null, null);
        doTestConfig(ZSTD, 1, LZ4, 1);
        doTestConfig(ZSTD, null, LZ4, null);

        doTestConfig(null, null, null, 2);
        doTestConfig(ZSTD, null, ZSTD, 2);
        doTestConfig(ZSTD, 1, ZSTD, 2);
    }

    /**
     */
    private void doTestConfig(
        DiskPageCompression compression1,
        Integer level1,
        DiskPageCompression compression2,
        Integer level2
    ) throws Exception {
        this.compression1 = compression1;
        this.level1 = level1;

        this.compression2 = compression2;
        this.level2 = level2;

        try {
            startGrid();

            fail("Exception expected.");
        }
        catch (IgniteCheckedException e) {
            if (compression1 != compression2)
                assertTrue(e.getCause().getMessage().startsWith("Disk page compression mismatch"));
            else if (!Objects.equals(level1, level2))
                assertTrue(e.getCause().getMessage().startsWith("Disk page compression level mismatch"));
            else
                fail(X.getFullStackTrace(e));
        }
    }
}
