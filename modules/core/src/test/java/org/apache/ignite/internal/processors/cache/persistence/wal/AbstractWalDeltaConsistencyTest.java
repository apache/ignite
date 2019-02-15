/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.persistence.wal;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.processors.cache.persistence.wal.memtracker.PageMemoryTrackerConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Abstract WAL delta records consistency test.
 */
public abstract class AbstractWalDeltaConsistencyTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(getDataStorageConfiguration());

        cfg.setPluginConfigurations(new PageMemoryTrackerConfiguration().setEnabled(true)
            .setCheckPagesOnCheckpoint(checkPagesOnCheckpoint()));

        return cfg;
    }

    /**
     * @param name Cache name.
     * @return Cache configuration.
     */
    @SuppressWarnings("unchecked")
    protected  <K,V> CacheConfiguration<K, V> cacheConfiguration(String name) {
        return defaultCacheConfiguration().setName(name);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * Check page memory on each checkpoint.
     */
    protected boolean checkPagesOnCheckpoint() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return checkPagesOnCheckpoint() ?
            new StopNodeFailureHandler() {
                @Override public boolean handle(Ignite ignite, FailureContext failureCtx) {
                    if (failureCtx.type() == FailureType.SYSTEM_WORKER_BLOCKED)
                        return false;

                    return super.handle(ignite, failureCtx);
                }
            } :
            super.getFailureHandler(igniteInstanceName);
    }

    /**
     * Default configuration contains one data region ('dflt-plc') with persistence enabled.
     * This method should be overridden by subclasses if another data storage configuration is needed.
     *
     * @return Data storage configuration used for starting of grid.
     */
    protected DataStorageConfiguration getDataStorageConfiguration() {
        return new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setInitialSize(256 * 1024 * 1024)
                .setMaxSize(256 * 1024 * 1024)
                .setPersistenceEnabled(true)
                .setName("dflt-plc"));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }
}
