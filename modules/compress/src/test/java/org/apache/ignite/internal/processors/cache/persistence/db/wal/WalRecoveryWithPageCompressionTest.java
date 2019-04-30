/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.testframework.junits.WithSystemProperty;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DEFAULT_DISK_PAGE_COMPRESSION;

/**
 * WAL recovery test with WAL page compression enabled and PDS disk page compression disabled.
 */
@WithSystemProperty(key = IGNITE_DEFAULT_DISK_PAGE_COMPRESSION, value = "DISABLED")
public class WalRecoveryWithPageCompressionTest extends IgniteWalRecoveryTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        walPageCompression = DiskPageCompression.ZSTD;
    }
}
