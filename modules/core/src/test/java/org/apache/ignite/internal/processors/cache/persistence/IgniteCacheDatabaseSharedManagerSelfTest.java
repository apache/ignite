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
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.lang.System.clearProperty;
import static java.lang.System.setProperty;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_THRESHOLD_WAL_ARCHIVE_SIZE_PERCENTAGE;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_WAL_SEGMENT_SIZE;
import static org.apache.ignite.configuration.DataStorageConfiguration.HALF_MAX_WAL_ARCHIVE_SIZE;
import static org.apache.ignite.configuration.DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE;
import static org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager.checkWalArchiveSizeConfiguration;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Class for testing {@link IgniteCacheDatabaseSharedManager}.
 */
public class IgniteCacheDatabaseSharedManagerSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        clearProperty(IGNITE_THRESHOLD_WAL_ARCHIVE_SIZE_PERCENTAGE);
    }

    /**
     * Checking the correctness of validation {@link DataStorageConfiguration#getMinWalArchiveSize()}.
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = IGNITE_THRESHOLD_WAL_ARCHIVE_SIZE_PERCENTAGE, value = "-1")
    public void testCheckMinWalArchiveSize() throws Exception {
        DataStorageConfiguration cfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            .setMaxWalArchiveSize(UNLIMITED_WAL_ARCHIVE);

        for (long i : F.asList(10L, 100L, HALF_MAX_WAL_ARCHIVE_SIZE))
            checkWalArchiveSizeConfiguration(cfg.setMinWalArchiveSize(i), log);

        int max = DFLT_WAL_SEGMENT_SIZE;
        cfg.setMaxWalArchiveSize(max);

        for (long i : F.asList(1L, 10L, HALF_MAX_WAL_ARCHIVE_SIZE, (long)max))
            checkWalArchiveSizeConfiguration(cfg.setMinWalArchiveSize(i), log);

        for (long i : F.asList(max * 2, max * 3)) {
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

    /**
     * Checking the correctness of validation
     * {@link IgniteSystemProperties#IGNITE_THRESHOLD_WAL_ARCHIVE_SIZE_PERCENTAGE}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCheckIgniteThresholdWalArchiveSizePercentage() throws Exception {
        DataStorageConfiguration cfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true))
            .setMaxWalArchiveSize(UNLIMITED_WAL_ARCHIVE)
            .setMinWalArchiveSize(HALF_MAX_WAL_ARCHIVE_SIZE);

        for (double i : F.asList(0.1d, 1d, (double)HALF_MAX_WAL_ARCHIVE_SIZE)) {
            setProperty(IGNITE_THRESHOLD_WAL_ARCHIVE_SIZE_PERCENTAGE, Double.toString(i));
            checkWalArchiveSizeConfiguration(cfg, log);
        }

        cfg.setMaxWalArchiveSize(DFLT_WAL_SEGMENT_SIZE);

        for (double i : F.asList(0.1d, 1d)) {
            setProperty(IGNITE_THRESHOLD_WAL_ARCHIVE_SIZE_PERCENTAGE, Double.toString(i));
            checkWalArchiveSizeConfiguration(cfg, log);
        }

        for (double i : F.asList(2d, 3d)) {
            setProperty(IGNITE_THRESHOLD_WAL_ARCHIVE_SIZE_PERCENTAGE, Double.toString(i));

            assertThrows(
                log,
                () -> {
                    checkWalArchiveSizeConfiguration(cfg, log);

                    return null;
                },
                IgniteCheckedException.class,
                null
            );
        }
    }
}
