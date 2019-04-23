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

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.DiskPageCompression;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DEFAULT_DISK_PAGE_COMPRESSION;

/**
 *
 */
public class WalRecoveryWithPageCompressionTest extends IgniteWalRecoveryTest {
    /** Old value of disk page compression system property. */
    private static DiskPageCompression oldDiskPageCompression;

    /**
     * We need to clear default DiskPageCompression system property (which is set up by default for this test suite)
     * to test WAL compression and data store compression separately.
     */
    @BeforeClass
    public static void clearSystemProperty() {
        oldDiskPageCompression = IgniteSystemProperties.getEnum(DiskPageCompression.class,
            IGNITE_DEFAULT_DISK_PAGE_COMPRESSION);

        System.clearProperty(IGNITE_DEFAULT_DISK_PAGE_COMPRESSION);
    }

    /**
     * Restore suite default DiskPageCompression system property.
     */
    @AfterClass
    public static void restoreSystemProperty() {
        if (oldDiskPageCompression != null)
            System.setProperty(IGNITE_DEFAULT_DISK_PAGE_COMPRESSION, oldDiskPageCompression.name());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        walPageCompression = DiskPageCompression.ZSTD;
    }
}
