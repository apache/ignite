/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.development.utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.encryption.AbstractEncryptionTest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.encryption.keystore.KeystoreEncryptionSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Collections.emptyList;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_BINARY_METADATA_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_MARSHALLER_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_ARCHIVE_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_PATH;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

/**
 * Class that contains tests on interaction between the {@link IgniteWalConverter} and encrypted WALs.
 */
public class IgniteEncryptedWalConverterTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanPersistenceDir();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        KeystoreEncryptionSpi encSpi = new KeystoreEncryptionSpi();

        encSpi.setKeyStorePath(AbstractEncryptionTest.KEYSTORE_PATH);
        encSpi.setKeyStorePassword(AbstractEncryptionTest.KEYSTORE_PASSWORD.toCharArray());

        return super.getConfiguration(igniteInstanceName)
            .setEncryptionSpi(encSpi)
            .setCacheConfiguration(defaultCacheConfiguration().setEncryptionEnabled(true))
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true))
            );
    }

    /**
     * Populates an encrypted cache and checks that its WAL contains encrypted records.
     */
    @Test
    public void testIgniteWalConverter() throws Exception {
        String nodeFolder = createWal();

        ByteArrayOutputStream outByte = new ByteArrayOutputStream();

        PrintStream out = new PrintStream(outByte);

        IgniteWalConverterArguments arg = new IgniteWalConverterArguments(
            U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_WAL_PATH, false),
            U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_WAL_ARCHIVE_PATH, false),
            DataStorageConfiguration.DFLT_PAGE_SIZE,
            new File(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_BINARY_METADATA_PATH, false), nodeFolder),
            U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_MARSHALLER_PATH, false),
            false,
            null,
            null,
            null,
            null,
            null,
            false,
            false,
            emptyList()
        );

        IgniteWalConverter.convert(out, arg);

        String result = outByte.toString();

        assertThat(result, containsString("EncryptedRecord"));
    }

    /**
     * Populates a cache and returns the name of its node's folder.
     */
    private String createWal() throws Exception {
        try (IgniteEx node = startGrid(0)) {
            node.cluster().state(ClusterState.ACTIVE);

            IgniteCache<Integer, Integer> cache = node.cache(DEFAULT_CACHE_NAME);

            for (int i = 0; i < 10; i++)
                cache.put(i, i);

            return node.context().pdsFolderResolver().resolveFolders().folderName();
        }
    }
}
