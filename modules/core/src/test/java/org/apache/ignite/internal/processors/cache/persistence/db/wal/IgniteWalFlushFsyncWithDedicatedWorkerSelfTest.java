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

import org.apache.ignite.IgniteSystemProperties;

/**
 *
 */
public class IgniteWalFlushFsyncWithDedicatedWorkerSelfTest extends IgniteWalFlushFsyncSelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_WAL_FSYNC_WITH_DEDICATED_WORKER, "true");

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        System.clearProperty(IgniteSystemProperties.IGNITE_WAL_FSYNC_WITH_DEDICATED_WORKER);
    }
}
