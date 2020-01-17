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
 *
 */

package org.apache.ignite.compatibility.persistence;

import java.util.Collection;
import java.util.Set;
import org.apache.ignite.compatibility.testframework.junits.Dependency;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.jetbrains.annotations.NotNull;

public class IndexingMigrationAbstractionTest extends IgnitePersistenceCompatibilityAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        new ConfigurationClosure().apply(cfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override @NotNull protected Collection<Dependency> getDependencies(String igniteVer) {
        Collection<Dependency> dependencies = super.getDependencies(igniteVer);

        dependencies.add(new Dependency("indexing","ignite-indexing", igniteVer, false));

        if (igniteVer.equals("2.7.0"))
            dependencies.add(new Dependency("h2", "com.h2database", "h2", "1.4.197", false));
        else
            dependencies.add(new Dependency("h2", "com.h2database", "h2", "1.4.195", false));

        return dependencies;
    }

    /** {@inheritDoc} */
    @Override protected Set<String> getExcluded(String ver, Collection<Dependency> dependencies) {
        Set<String> excluded = super.getExcluded(ver, dependencies);

        excluded.add("h2");

        return excluded;
    }

    /** */
    protected class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            cfg.setLocalHost("127.0.0.1");

            TcpDiscoverySpi disco = new TcpDiscoverySpi();
            disco.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);

            cfg.setDiscoverySpi(disco);

            cfg.setPeerClassLoadingEnabled(false);

            DataStorageConfiguration memCfg = new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true)
                        .setInitialSize(1024 * 1024 * 10).setMaxSize(1024 * 1024 * 15))
                .setSystemRegionInitialSize(1024 * 1024 * 10)
                .setSystemRegionMaxSize(1024 * 1024 * 15);

            cfg.setDataStorageConfiguration(memCfg);
        }
    }
}
