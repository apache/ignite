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

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalRebalanceTest;

/**
 *
 */
public class IgnitePdsTxHistoricalRebalancingTest extends IgnitePdsTxCacheRebalancingTest {
    /** {@inheritDoc */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCommunicationSpi(new IgniteWalRebalanceTest.WalRebalanceCheckingCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc */
    @Override protected long checkpointFrequency() {
        return 15 * 1000;
    }

    /** {@inheritDoc */
    @Override protected void beforeTest() throws Exception {
        // Use rebalance from WAL if possible.
        System.setProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "0");

        super.beforeTest();
    }

    /** {@inheritDoc */
    @Override protected void afterTest() throws Exception {
        boolean walRebalanceInvoked = !IgniteWalRebalanceTest.WalRebalanceCheckingCommunicationSpi.allRebalances()
            .isEmpty();

        IgniteWalRebalanceTest.WalRebalanceCheckingCommunicationSpi.cleanup();

        System.clearProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD);

        super.afterTest();

        if (!walRebalanceInvoked)
            throw new AssertionError("WAL rebalance hasn't been invoked.");
    }
}
