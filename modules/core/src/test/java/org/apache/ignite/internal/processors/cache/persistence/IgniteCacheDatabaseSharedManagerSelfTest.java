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

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_SEGMENT_SIZE;
import static org.apache.ignite.configuration.DataStorageConfiguration.HALF_MAX_WAL_ARCHIVE_SIZE;
import static org.apache.ignite.configuration.DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE;
import static org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager.checkWalArchiveSizeConfiguration;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Class for testing {@link IgniteCacheDatabaseSharedManager}.
 */
public class IgniteCacheDatabaseSharedManagerSelfTest extends GridCommonAbstractTest {
    /**
     * Checking the correctness of validation {@link DataStorageConfiguration#getMinWalArchiveSize()}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCheckMinWalArchiveSize() throws Exception {
        DataStorageConfiguration cfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true));

        cfg.setMaxWalArchiveSize(UNLIMITED_WAL_ARCHIVE);

        for (long i : F.asList(10L, 100L, HALF_MAX_WAL_ARCHIVE_SIZE))
            checkWalArchiveSizeConfiguration(cfg.setMinWalArchiveSize(i), log);

        cfg.setMaxWalArchiveSize(DFLT_WAL_SEGMENT_SIZE);

        for (long i : F.asList(1L, 10L, HALF_MAX_WAL_ARCHIVE_SIZE, (long)DFLT_WAL_SEGMENT_SIZE))
            checkWalArchiveSizeConfiguration(cfg.setMinWalArchiveSize(i), log);

        for (long i : F.asList(DFLT_WAL_SEGMENT_SIZE * 2, DFLT_WAL_SEGMENT_SIZE * 3)) {
            assertThrows(
                log,
                () -> {
                    checkWalArchiveSizeConfiguration(cfg.setMinWalArchiveSize(i), log);

                    return null;
                },
                IgniteCheckedException.class,
                null
            );
        }
    }
}
