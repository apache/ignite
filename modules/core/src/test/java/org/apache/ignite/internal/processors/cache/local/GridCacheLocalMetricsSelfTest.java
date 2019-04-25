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

package org.apache.ignite.internal.processors.cache.local;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheTransactionalAbstractMetricsSelfTest;
import org.apache.ignite.testframework.MvccFeatureChecker;

import static org.apache.ignite.cache.CacheMode.LOCAL;

/**
 * Local cache metrics test.
 */
public class GridCacheLocalMetricsSelfTest extends GridCacheTransactionalAbstractMetricsSelfTest {
    /** */
    private static final int GRID_CNT = 1;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.LOCAL_CACHE);
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.METRICS);

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        c.getTransactionConfiguration().setTxSerializableEnabled(true);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.LOCAL_CACHE);
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.METRICS);

        CacheConfiguration cfg = super.cacheConfiguration(igniteInstanceName);

        cfg.setCacheMode(LOCAL);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }
}
