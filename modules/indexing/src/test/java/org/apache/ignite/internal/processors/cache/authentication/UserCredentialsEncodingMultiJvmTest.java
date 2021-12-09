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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.authentication.User.DFAULT_USER_NAME;
import static org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction.IDX_ATTR;

/** Tests that authenticator is independent of default JVM character encoding. */
@RunWith(Parameterized.class)
public class UserCredentialsEncodingMultiJvmTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameter()
    public boolean isSurrogatesCharactersSupportEnabled;

    /** */
    @Parameterized.Parameters(name = "isSurrogatesCharactersSupportEnabled={0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[] {true}, new Object[] {false});
    }

    /** */
    public static final List<String> TEST_ENCODINGS = Arrays.asList("Big5", "UTF-8");

    /** */
    public static final List<SecurityCredentials> TEST_CREDENTIALS = Arrays.asList(
        new SecurityCredentials("login", "pwd", false),
        new SecurityCredentials("語", "Ignite是有史以來最好的基地", false),
        new SecurityCredentials("的的abcd123кириллица", "的的abcd123пароль", false),
        new SecurityCredentials(new String(new char[] {55296}), new String(new char[] {0xD800, '的', 0xD800, 0xD800, 0xDC00, 0xDFFF}), true),
        new SecurityCredentials(new String(new char[] {0xD800, '的', 0xD800, 0xD800, 0xDC00, 0xDFFF}), new String(new char[] {55296}), true)
    );

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

        stopAllGrids();

        IgniteProcessProxy.killAll();

        cleanPersistenceDir();
    }

    /** Tests that Ignite nodes that are running on JVMs with different encodings handles users in the same way. */
    @Test
    public void testNodesWithDifferentEncodings() throws Exception {
        int nodesCnt = 0;

        for (String encoding : TEST_ENCODINGS) {
            startNodeProcess(nodesCnt++, false, encoding);
            startNodeProcess(nodesCnt++, true, encoding);
        }

        prepareCluster(nodesCnt, true);

        for (String encoding : TEST_ENCODINGS)
            runProcess(UserCreator.class, encoding, Integer.toString(nodesCnt));

        for (String encoding : TEST_ENCODINGS)
            runProcess(TestClientConnector.class, encoding, Integer.toString(nodesCnt));

        IgniteProcessProxy.killAll();

        nodesCnt = 0;

        for (String encoding : TEST_ENCODINGS) {
            startNodeProcess(nodesCnt++, false, encoding);
            startNodeProcess(nodesCnt++, true, encoding);
        }

        prepareCluster(nodesCnt, false);

        for (String encoding : TEST_ENCODINGS)
            runProcess(TestClientConnector.class, encoding, Integer.toString(nodesCnt));
    }

    /** Creates test users. */
    public static class UserCreator {
        /** */
        public static void main(String[] args) throws Exception {
            String procEncoding = System.getProperty("file.encoding");

            int nodesCnt = Integer.parseInt(args[0]);

            for (SecurityCredentials cred : TEST_CREDENTIALS) {
                if (containsSurrogates(cred) && !BinaryUtils.USE_STR_SERIALIZATION_VER_2)
                    continue;

                for (int creatorNodeIdx = 0; creatorNodeIdx < nodesCnt; creatorNodeIdx++) {
                    String login = getLogin(cred, creatorNodeIdx, procEncoding);
                    String pwd = (String)cred.getPassword();

                    try (IgniteClient cli = connectClient(creatorNodeIdx, DFAULT_USER_NAME, "ignite")) {
                        cli.query(new SqlFieldsQuery(
                            String.format("CREATE USER \"%s\" WITH PASSWORD '%s';", login, pwd))
                        ).getAll();
                    }
                }
            }
        }
    }

    /** */
    public static class TestClientConnector {
        /** */
        public static void main(String[] args) throws Exception {
            String procEncoding = System.getProperty("file.encoding");

            int nodesCnt = Integer.parseInt(args[0]);

            for (SecurityCredentials cred : TEST_CREDENTIALS) {
                if (containsSurrogates(cred) && !BinaryUtils.USE_STR_SERIALIZATION_VER_2)
                    continue;

                for (int creatorNodeIdx = 0; creatorNodeIdx < nodesCnt; creatorNodeIdx++) {
                    String login = getLogin(cred, creatorNodeIdx, procEncoding);
                    String pwd = (String)cred.getPassword();

                    for (int nodeIdx = 0; nodeIdx < nodesCnt; nodeIdx++) {
                        X.println("Testing client connection [credIdx=" + TEST_CREDENTIALS.indexOf(cred) +
                            ", userCreatorNodeIdx=" + creatorNodeIdx +
                            ", procEncoding=" + procEncoding +
                            ", clientConnectorNodeIdx=" + nodeIdx
                        );

                        try (IgniteClient cli = connectClient(nodeIdx, login, pwd)) {
                            ClientCache<Object, Object> cache = cli.cache(DEFAULT_CACHE_NAME);

                            for (int i = 0; i < nodesCnt; i++) {
                                cache.put(i, i);

                                assertEquals(i, cache.get(i));
                            }
                        }
                    }
                }
            }
        }
    }

    /** */
    private static String getLogin(SecurityCredentials creds, int nodeIdx, String procEncoding) {
        return (String)creds.getLogin() + '_' + nodeIdx + '_' + procEncoding;
    }

    /** */
    private static boolean containsSurrogates(SecurityCredentials cred) {
        return (Boolean)cred.getUserObject();
    }

    /** */
    private IgniteProcessProxy startNodeProcess(int idx, boolean isClient, String encoding) throws Exception {
        return new IgniteProcessProxy(
            getConfiguration(getTestIgniteInstanceName(idx)).setClientMode(isClient),
            log,
            null,
            false,
            parameters("-Dfile.encoding=" + encoding)
        );
    }

    /** */
    private void runProcess(Class<?> runner, String encoding, String... args) throws Exception {
        GridJavaProcess proc = GridJavaProcess.exec(
            runner.getName(),
            String.join(" ", args),
            log,
            log::info,
            null,
            null,
             parameters("-ea", "-DIGNITE_QUIET=false", "-Dfile.encoding=" + encoding),
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
        String prev = System.getProperty(IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2);

        System.setProperty(IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2, Boolean.toString(isSurrogatesCharactersSupportEnabled));

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
        finally {
            if (prev != null)
                System.setProperty(IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2, prev);
        }
    }

    /** */
    private List<String> parameters(String... params) {
        List<String> res = new ArrayList<>(Arrays.asList(params));

        if (isSurrogatesCharactersSupportEnabled)
            res.add("-D" + IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2 + "=true");

        return res;
    }

    /** */
    private static IgniteClient connectClient(int nodeIdx, String login, String pwd) {
        return Ignition.startClient(new ClientConfiguration()
            .setAddresses("127.0.0.1:1080" + nodeIdx)
            .setUserName(login)
            .setUserPassword(pwd));
    }
}
