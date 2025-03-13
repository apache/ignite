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
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.compatibility.IgniteReleasedVersion;
import org.apache.ignite.compatibility.testframework.junits.Dependency;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.jetbrains.annotations.NotNull;

/**
 * This class contains basic settings for indexes compatibility tests.
 */
public abstract class IndexAbstractCompatibilityTest extends IgnitePersistenceCompatibilityAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
                )
                // Disable WAL to skip filling index with reading WAL. Instead just start on previous persisted files.
                .setWalMode(WALMode.NONE));

        cfg.setBinaryConfiguration(
            new BinaryConfiguration()
                .setCompactFooter(true)
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override @NotNull protected Collection<Dependency> getDependencies(String igniteVer) {
        Collection<Dependency> dependencies = super.getDependencies(igniteVer);

        if (IgniteProductVersion.fromString(igniteVer).compareTo(IgniteReleasedVersion.VER_2_7_0.version()) < 0) {
            dependencies.add(new Dependency("h2", "com.h2database", "h2", "1.4.195", false));

            dependencies.add(new Dependency("h2", "org.apache.lucene", "lucene-core", "5.5.2", false));
            dependencies.add(new Dependency("h2", "org.apache.lucene", "lucene-analyzers-common", "5.5.2", false));
            dependencies.add(new Dependency("h2", "org.apache.lucene", "lucene-queryparser", "5.5.2", false));
        }

        dependencies.add(new Dependency("indexing", "ignite-indexing", false));

        return dependencies;
    }

    /** {@inheritDoc} */
    @Override protected Set<String> getExcluded(String ver, Collection<Dependency> dependencies) {
        Set<String> excluded = super.getExcluded(ver, dependencies);

        if (IgniteProductVersion.fromString(ver).compareTo(IgniteReleasedVersion.VER_2_7_0.version()) < 0)
            excluded.add("h2");

        return excluded;
    }

    /** */
    protected void checkIndexUsed(IgniteCache<?, ?> cache, SqlFieldsQuery qry, String idxName) {
        assertTrue("Query does not use index.", queryPlan(cache, qry).toLowerCase().contains(idxName.toLowerCase()));
    }

    /**
     * Run SQL statement on specified node.
     *
     * @param node node to execute query.
     * @param stmt Statement to run.
     * @param args arguments of statements
     * @return Run result.
     */
    protected static List<List<?>> executeSql(IgniteEx node, String stmt, Object... args) {
        return node.context().query().querySqlFields(new SqlFieldsQuery(stmt).setArgs(args), true).getAll();
    }

    /** */
    public static class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** Compact footer. */
        private boolean compactFooter;

        /**
         * @param compactFooter Compact footer.
         */
        public ConfigurationClosure(boolean compactFooter) {
            this.compactFooter = compactFooter;
        }

        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            cfg.setLocalHost("127.0.0.1");

            TcpDiscoverySpi disco = new TcpDiscoverySpi();
            disco.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);

            cfg.setDiscoverySpi(disco);

            cfg.setPeerClassLoadingEnabled(false);

            cfg.setPersistentStoreConfiguration(new PersistentStoreConfiguration());

            cfg.setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(compactFooter));
        }
    }
}
