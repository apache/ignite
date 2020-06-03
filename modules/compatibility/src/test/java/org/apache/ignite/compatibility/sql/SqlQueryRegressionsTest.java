/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.compatibility.sql;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.compatibility.sql.model.City;
import org.apache.ignite.compatibility.sql.model.Company;
import org.apache.ignite.compatibility.sql.model.Country;
import org.apache.ignite.compatibility.sql.model.Department;
import org.apache.ignite.compatibility.sql.model.ModelFactory;
import org.apache.ignite.compatibility.sql.model.Person;
import org.apache.ignite.compatibility.sql.util.PredefinedQueriesSupplier;
import org.apache.ignite.compatibility.sql.util.QueryDuelBenchmark;
import org.apache.ignite.compatibility.sql.util.QueryDuelResult;
import org.apache.ignite.compatibility.sql.util.SimpleConnectionPool;
import org.apache.ignite.compatibility.testframework.junits.Dependency;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 * Test for SQL queries regressions detection.
 * It happens in the next way:
 * 
 * 1. Test starts two different Ignite versions: current version and the old one.
 * 2. Then framework executes (randomly chosen/generated) equivalent queries in both versions.
 * 3. Execution time for both version is measured and if it exceeds some threshold, the query marked as suspected.
 * 4. All suspected queries are submitted to both Ignite versions one more time to get rid of outliers.
 * 5. If a poor execution time is reproducible for suspected query,
 *    this query is reported as a problematic and test fails because of it.
 */
@SuppressWarnings("TypeMayBeWeakened")
public class SqlQueryRegressionsTest extends IgniteCompatibilityAbstractTest {
    /** Ignite version. */
    private static final String IGNITE_VERSION = "2.5.0";

    /** */
    private static final int OLD_JDBC_PORT = 10800;

    /** */
    private static final int NEW_JDBC_PORT = 10802;

    /** Query workers count. */
    private static final int WORKERS_CNT = IgniteConfiguration.DFLT_QUERY_THREAD_POOL_SIZE / 2;

    /** */
    private static final long TEST_TIMEOUT = 60_000;

    /** */
    private static final long WARM_UP_TIMEOUT = 5_000;

    /** */
    private static final String JDBC_URL = "jdbc:ignite:thin://127.0.0.1:";

    /** */
    private static final TcpDiscoveryIpFinder OLD_VER_FINDER = new TcpDiscoveryVmIpFinder(true) {{
        setAddresses(Collections.singleton("127.0.0.1:47500..47509"));
    }};

    /**  */
    private static final TcpDiscoveryVmIpFinder NEW_VER_FINDER = new TcpDiscoveryVmIpFinder(true) {{
        setAddresses(Collections.singleton("127.0.0.1:47510..47519"));
    }};

    /** Default queries.. */
    private final Supplier<String> qrysSupplier = new PredefinedQueriesSupplier(Arrays.asList(
        "SELECT * FROM person p1 WHERE id > 0",
        "SELECT * FROM department d1 WHERE id > 0",
        "SELECT * FROM country c1",
        "SELECT * FROM city ci1",
        "SELECT * FROM company co1",
        "SELECT * FROM person p, department d, company co " +
            "WHERE p.depId=d.id AND d.companyId = co.id",
        "SELECT * FROM person p, department d, company co, city ci " +
                    "WHERE p.depId=d.id AND d.companyId = co.id AND co.cityId = ci.id",
        "SELECT * FROM person p, department d, company co, city ci " +
            "WHERE p.depId=d.id AND d.companyId = co.id AND p.cityId = ci.id",
        "SELECT * FROM person p, department d, company co " +
            "WHERE p.depId=d.id AND d.companyId = co.id AND d.companyId > 50",
        "SELECT * FROM person p, department d, company co, city ci " +
            "WHERE p.depId=d.id AND d.companyId = co.id AND co.cityId = ci.id AND d.companyId > 50 AND d.id < 80",
        "SELECT * FROM person p, department d, company co, city ci " +
            "WHERE p.depId=d.id AND d.companyId = co.id AND p.cityId = ci.id AND d.cityId > 10 AND co.headCnt < 20"
    ));

    /** {@inheritDoc} */
    @Override protected @NotNull Collection<Dependency> getDependencies(String igniteVer) {
        Collection<Dependency> dependencies = super.getDependencies(igniteVer);

        dependencies.add(new Dependency("indexing", "ignite-indexing", false));

        dependencies.add(new Dependency("h2", "com.h2database", "h2", "1.4.195", false));

        return dependencies;
    }

    /** {@inheritDoc} */
    @Override protected Set<String> getExcluded(String ver, Collection<Dependency> dependencies) {
        Set<String> excluded = super.getExcluded(ver, dependencies);

        excluded.add("h2");

        return excluded;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 2 * TEST_TIMEOUT + WARM_UP_TIMEOUT + super.getTestTimeout();
    }

    /**
     * Test for SQL performance regression detection.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSqlPerformanceRegressions() throws Exception {
        try {
            int seed = ThreadLocalRandom.current().nextInt();

            if (log.isInfoEnabled())
                log.info("Chosen random seed=" + seed);

            startOldAndNewClusters(seed);

            createTablesAndPopulateData(grid(0), seed);

            try (SimpleConnectionPool oldConnPool = new SimpleConnectionPool(JDBC_URL, OLD_JDBC_PORT, WORKERS_CNT);
                 SimpleConnectionPool newConnPool = new SimpleConnectionPool(JDBC_URL, NEW_JDBC_PORT, WORKERS_CNT)) {
                QueryDuelBenchmark benchmark = new QueryDuelBenchmark(WORKERS_CNT, oldConnPool, newConnPool);
                // 0. Warm-up.
                benchmark.runBenchmark(WARM_UP_TIMEOUT, qrysSupplier, 0, 1);

                // 1. Initial run.
                Collection<QueryDuelResult> suspiciousQrys =
                    benchmark.runBenchmark(TEST_TIMEOUT, qrysSupplier, 1, 1);

                if (suspiciousQrys.isEmpty())
                    return; // No suspicious queries - no problem.

                Set<String> suspiciousQrysSet = suspiciousQrys.stream()
                    .map(QueryDuelResult::query)
                    .collect(Collectors.toSet());

                if (log.isInfoEnabled())
                    log.info("Problematic queries number: " + suspiciousQrysSet.size());

                Supplier<String> problematicQrysSupplier = new PredefinedQueriesSupplier(suspiciousQrysSet);

                // 2. Rerun problematic queries to ensure they are not outliers.
                Collection<QueryDuelResult> failedQueries =
                    benchmark.runBenchmark(WARM_UP_TIMEOUT, problematicQrysSupplier, 3, 5);

                assertTrue("Found SQL performance regression for queries: " + formatPretty(failedQueries),
                    failedQueries.isEmpty());
            }
        }
        finally {
            stopClusters();
        }
    }

    /**
     * Starts old and new Ignite clusters.
     * @param seed Random seed.
     */
    public void startOldAndNewClusters(int seed) throws Exception {
        // Old cluster.
        startGrid(2, IGNITE_VERSION, new NodeConfigurationClosure(), new PostStartupClosure(true, seed));
        startGrid(3, IGNITE_VERSION, new NodeConfigurationClosure(), new PostStartupClosure(false, seed));

        // New cluster
        IgnitionEx.start(prepareNodeConfig(
            getConfiguration(getTestIgniteInstanceName(0)), NEW_VER_FINDER, NEW_JDBC_PORT));
        IgnitionEx.start(prepareNodeConfig(
            getConfiguration(getTestIgniteInstanceName(1)), NEW_VER_FINDER, NEW_JDBC_PORT));
    }

    /**
     * Stops both new and old clusters.
     */
    public void stopClusters() {
        // Old cluster.
        IgniteProcessProxy.killAll();

        // New cluster.
        for (Ignite ignite : G.allGrids())
            U.close(ignite, log);
    }

    /**
     * @param qrys Queries duels result.
     * @return Pretty formatted result of duels.
     */
    private static String formatPretty(Collection<QueryDuelResult> qrys) {
        StringBuilder sb = new StringBuilder().append("\n");

        for (QueryDuelResult res : qrys) {
            sb.append(res)
                .append('\n');
        }

        return sb.toString();
    }

    /**
     * @param ignite Ignite node.
     * @param seed Random seed.
     */
    private static void createTablesAndPopulateData(Ignite ignite, int seed) {
        createAndPopulateTable(ignite, new Person.Factory(seed));
        createAndPopulateTable(ignite, new Department.Factory(seed));
        createAndPopulateTable(ignite, new Country.Factory(seed));
        createAndPopulateTable(ignite, new City.Factory(seed));
        createAndPopulateTable(ignite, new Company.Factory(seed));
    }

    /** */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void createAndPopulateTable(Ignite ignite, ModelFactory factory) {
        QueryEntity qryEntity = factory.queryEntity();
        CacheConfiguration cacheCfg = new CacheConfiguration<>(factory.tableName())
            .setQueryEntities(Collections.singleton(qryEntity))
            .setSqlSchema("PUBLIC");

        IgniteCache personCache = ignite.createCache(cacheCfg);

        for (long i = 0; i < factory.count(); i++)
            personCache.put(i, factory.createRandom());
    }

    /**
     * Prepares ignite nodes configuration.
     */
    private static IgniteConfiguration prepareNodeConfig(IgniteConfiguration cfg, TcpDiscoveryIpFinder ipFinder,
        int jdbcPort) {
        cfg.setLocalHost("127.0.0.1");
        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();
        disco.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(disco);

        ClientConnectorConfiguration clientCfg = new ClientConnectorConfiguration();
        clientCfg.setPort(jdbcPort);
        return cfg;
    }

    /**
     * Configuration closure.
     */
    private static class NodeConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            prepareNodeConfig(cfg, OLD_VER_FINDER, OLD_JDBC_PORT);
        }
    }

    /**
     * Closure that executed for old Ingite version after start up.
     */
    private static class PostStartupClosure implements IgniteInClosure<Ignite> {
        /** */
        private final boolean createTbl;

        /** Random seed. */
        private final int seed;

        /**
         * @param createTbl {@code true} In case table should be created
         * @param seed
         */
        PostStartupClosure(boolean createTbl, int seed) {
            this.createTbl = createTbl;
            this.seed = seed;
        }

        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            if (createTbl) {
                createTablesAndPopulateData(ignite, seed);
            }
        }
    }
}
