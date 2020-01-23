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

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.MAX_PAGE_SIZE;
import static org.apache.ignite.configuration.WALMode.FSYNC;

/**
 * Test verifies that Disk Page Compression feature cannot be used together with Encryption feature,
 * corresponding validation rule is triggered when an attempt to start cache with such configuration is made.
 */
public class DiskPageCompressionConfigValidationTest extends GridCommonAbstractTest {
    /** */
    private static final String KEYSTORE_PATH =
        IgniteUtils.resolveIgnitePath("modules/compress/src/test/resources/encryption.jks").getAbsolutePath();

    /** */
    private static final String KEYSTORE_PASSWORD = "encryption";

    /** */
    private static final String INVALID_CFG_CACHE_NAME = "encrypted_with_compression";

    /** */
    private CacheConfiguration ccfg;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        KeystoreEncryptionSpi encSpi = new KeystoreEncryptionSpi();

        encSpi.setKeyStorePath(KEYSTORE_PATH);
        encSpi.setKeyStorePassword(KEYSTORE_PASSWORD.toCharArray());

        cfg.setEncryptionSpi(encSpi);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(10L * 1024 * 1024)
                    .setPersistenceEnabled(true))
            .setPageSize(MAX_PAGE_SIZE)
            .setWalMode(FSYNC);

        cfg.setDataStorageConfiguration(dsCfg);

        if (ccfg != null)
            cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * Test verifies that dynamic cache request with incorrect configuration (encryption and disk page compression
     * are mixed together) is rejected, corresponding exception is thrown.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectDynamicCacheStartRequest() throws Exception {
        Ignite ig = startGrid();

        ig.cluster().active(true);

        boolean expEThrown = false;

        try {
            ig.createCache(new CacheConfiguration<>(INVALID_CFG_CACHE_NAME)
                .setEncryptionEnabled(true)
                .setDiskPageCompression(DiskPageCompression.ZSTD)
            );
        }
        catch (Exception e) {
            List<Throwable> suppressedExceptions = X.getSuppressedList(e);

            if (suppressedExceptions.size() == 1) {
                Throwable cause = suppressedExceptions.get(0).getCause();

                if (cause != null && cause.getMessage() != null && cause.getMessage()
                    .startsWith("Encryption cannot be used with disk page compression"))
                    expEThrown = true;
                else
                    throw e;
            }
            else
                throw e;
        }

        assertTrue("Expected exception about cache configuration validation has not been thrown", expEThrown);
    }

    /**
     * Test verifies that node with incorrect static configuration (encryption and disk page compression
     * are mixed together) fails to start, corresponding exception is thrown.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectStaticCacheConfiguration() throws Exception {
        ccfg = new CacheConfiguration(INVALID_CFG_CACHE_NAME)
            .setEncryptionEnabled(true)
            .setDiskPageCompression(DiskPageCompression.ZSTD);

        boolean expEThrown = false;

        try {
            startGrid();
        }
        catch (Exception e) {
            String msg = e.getMessage();

            if (msg != null && msg.contains("Encryption cannot be used with disk page compression"))
                expEThrown = true;
            else
                throw e;
        }

        assertTrue("Expected exception about cache configuration validation has not been thrown", expEThrown);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }
}
