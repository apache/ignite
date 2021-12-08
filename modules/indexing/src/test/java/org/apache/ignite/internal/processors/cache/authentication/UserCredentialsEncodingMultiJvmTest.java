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

package org.apache.ignite.internal.processors.cache.authentication;

import java.util.Arrays;
import java.util.Collections;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.authentication.User.DFAULT_USER_NAME;
import static org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction.IDX_ATTR;

/** Tests that authenticator is independent of default JVM character encoding. */
public class UserCredentialsEncodingMultiJvmTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(LOCAL_IP_FINDER);

        int nodeIdx = getTestIgniteInstanceIndex(igniteInstanceName);

        cfg.setUserAttributes(Collections.singletonMap(IDX_ATTR, nodeIdx));
        cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration()
            .setPort(10800 + nodeIdx));

        cfg.setAuthenticationEnabled(true);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(100 * (1 << 20))
                .setPersistenceEnabled(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        IgniteProcessProxy.killAll();
    }

    /**
     *  Tests that Ignite nodes that are running on JVMs with different encodings handles users in the same way.
     */
    @Test
    public void testNodesWithDifferentEncodings() throws Exception {
        startNodeProcess(0, false, "UTF-8");
        startNodeProcess(1, false, "Big5");
        startNodeProcess(2, true, "UTF-8");
        startNodeProcess(3, true, "Big5");

        prepareCluster(4, true);

        startProcess(TestRunner.class, "Big5");
        startProcess(TestRunner.class, "UTF-8");
    }

    /** Tests that security user credentials are restored from PDS properly and are accessible. */
    @Test
    public void testClusterRestart() throws Exception {
        startNodeProcess(0, false, "Big5");

        prepareCluster(1, true);

        String login = "語";
        String pwd = "Ignite是有史以來最好的基地";

        runQueryWithSuperUser(0, String.format("CREATE USER \"%s\" WITH PASSWORD '%s';", login, pwd));

        IgniteProcessProxy.killAll();

        startNodeProcess(0, false, "UTF-8");

        prepareCluster(1, false);

        try (IgniteClient cli = connectClient(0, login, pwd)) {
            ClientCache<Object, Object> cache = cli.cache(DEFAULT_CACHE_NAME);

            cache.put(0, 0);
        }
    }

    /** Tests that user can pass any kind of symbols as a login and checks that Ignite treats it properly. */
    public static class TestRunner {
        /** */
        public static void main(String[] args) throws Exception {
            doTest("login", "pwd");
            doTest("語", "Ignite是有史以來最好的基地");
            doTest("的的abcd123кириллица", "的的abcd123пароль");
            doTest(new String(new char[]{55296}), new String(new char[] {0xD800, '的', 0xD800, 0xD800, 0xDC00, 0xDFFF}));
            doTest(new String(new char[] {0xD800, '的', 0xD800, 0xD800, 0xDC00, 0xDFFF}), new String(new char[]{55296}));
        }

        /** */
        private static void doTest(String login, String pwd) {
            String encoding = System.getProperty("file.encoding");

            for (int nodeIdx = 0; nodeIdx < 4; nodeIdx++) {
                String extendedLogin = login + '_' + nodeIdx + '_' + encoding;

                runQueryWithSuperUser(nodeIdx, String.format("CREATE USER \"%s\" WITH PASSWORD '%s';", extendedLogin, pwd));

                try (IgniteClient cli = connectClient(nodeIdx, extendedLogin, pwd)) {
                    ClientCache<Object, Object> cache = cli.cache(DEFAULT_CACHE_NAME);

                    for (int i = 0; i < 2; i++)
                        cache.put(i, i);
                }
            }
        }
    }

    /** */
    private IgniteProcessProxy startNodeProcess(int idx, boolean isClient, String encoding) throws Exception {
        return new IgniteProcessProxy(
            getConfiguration(getTestIgniteInstanceName(idx)).setClientMode(isClient),
            log,
            null,
            false,
            Collections.singletonList("-Dfile.encoding=" + encoding)
        );
    }

    /** */
    private void startProcess(Class<?> runner, String encoding, String... args) throws Exception {
        GridJavaProcess proc = GridJavaProcess.exec(
            runner.getName(),
            String.join(" ", args),
            log,
            log::info,
            null,
            null,
            Arrays.asList("-ea", "-DIGNITE_QUIET=false", "-Dfile.encoding=" + encoding),
            null);

        try {
            GridTestUtils.waitForCondition(() -> !proc.getProcess().isAlive(), getTestTimeout());

            assertEquals(0, proc.getProcess().exitValue());
        }
        finally {
            if (proc.getProcess().isAlive())
                proc.kill();
        }
    }

    /** */
    private void prepareCluster(int nodesCnt, boolean createTestCache) throws Exception {
        try (IgniteEx cli = startClientGrid(nodesCnt)) {
            assertTrue(GridTestUtils.waitForCondition(() -> cli.cluster().nodes().size() == nodesCnt + 1, getTestTimeout()));

            cli.cluster().state(ACTIVE);

            if (createTestCache) {
                int serversCnt = cli.cluster().forServers().nodes().size();

                cli.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                    .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                    .setAffinity(new GridCacheModuloAffinityFunction(serversCnt, serversCnt)));
            }
        }
    }

    /** */
    private static void runQueryWithSuperUser(int nodeIdx, String qry) {
        try (IgniteClient cli = connectClient(nodeIdx, DFAULT_USER_NAME, "ignite")) {
            cli.query(new SqlFieldsQuery(qry)).getAll();
        }
    }

    /** */
    private static IgniteClient connectClient(int nodeIdx, String login, String pwd) {
        return Ignition.startClient(new ClientConfiguration()
            .setAddresses("127.0.0.1:1080" + nodeIdx)
            .setUserName(login)
            .setUserPassword(pwd));
    }

}
