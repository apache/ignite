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

package org.apache.ignite.internal;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_EXCHANGE_HISTORY_SIZE;

/**
 * Test exchange history size parameter effect.
 */
@RunWith(JUnit4.class)
public class GridCachePartitionExchangeManagerHistSizeTest extends GridCommonAbstractTest {
    /** */
    private String oldHistVal;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        oldHistVal = System.getProperty(IGNITE_EXCHANGE_HISTORY_SIZE);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        if (oldHistVal != null)
            System.setProperty(IGNITE_EXCHANGE_HISTORY_SIZE, oldHistVal);
        else
            System.clearProperty(IGNITE_EXCHANGE_HISTORY_SIZE);
    }


    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSingleExchangeHistSize() throws Exception {
        System.setProperty(IGNITE_EXCHANGE_HISTORY_SIZE, "1");

        startGridsMultiThreaded(10);
    }
}
