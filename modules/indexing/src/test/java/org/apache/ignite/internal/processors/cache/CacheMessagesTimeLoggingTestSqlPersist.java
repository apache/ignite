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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.WithSystemProperty;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_MESSAGES_TIME_LOGGING;

/**
 * Checks messages network time logging with persistence.
 */
@WithSystemProperty(key = IGNITE_ENABLE_MESSAGES_TIME_LOGGING, value = "true")
public class CacheMessagesTimeLoggingTestSqlPersist extends CacheMessagesTimeLoggingTestSql {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dscfg = new DataStorageConfiguration();

        dscfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);

        cfg.setDataStorageConfiguration(dscfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        grid(0).cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void additionalChecks() throws Exception {
        super.additionalChecks();

        startGrid(GRID_CNT);

        resetBaselineTopology();

        awaitPartitionMapExchange();

        checkTimeLoggableMsgsConsistancy();
    }
}
