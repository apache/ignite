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

package org.apache.ignite.internal.processors.security;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.Ignition;
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
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.authentication.AuthenticationProcessorSelfTest.authenticate;
import static org.apache.ignite.internal.processors.authentication.User.DFAULT_USER_NAME;
import static org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction.IDX_ATTR;

/** 
 * Tests that Ignite nodes that are running on JVMs with different encodings and with authentication Enabled can
 * successfully obtain user security context by its security subject ID.
 */
public class AuthenticatorSecurityContextEncodingMultiJvmTest extends GridCommonAbstractTest {
    /** */
    private static final List<SecurityCredentials> TEST_CREDENTIALS = Arrays.asList(
        new SecurityCredentials("login", "pwd" ),
        new SecurityCredentials("語", "Ignite是有史以來最好的基地"),
        new SecurityCredentials("的的abcd123кириллица", "的的abcd123пароль"),
        new SecurityCredentials(new String(new char[] {55296}), new String(new char[] {0xD801})),
        new SecurityCredentials(
            new String(new char[] {0xD800, '的', 0xD800, 0xD800, 0xDC00, 0xDFFF}), 
            new String(new char[] {55296})
        )
    );

    /** */
    private static String remoteJvmEncoding;

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

    /**
     * Tests that Ignite nodes that are running on JVMs with different encodings can successfully obtain user security
     * context by its security subject ID.
     */
    @Test
    @WithSystemProperty(key = IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2, value = "true")
    public void testSecurityContextOnNodesWithDifferentEncodings() throws Exception {
        startGrid(0);

        startNodeProcess(1, "UTF-8");
        startNodeProcess(2, "Big5");

        grid(0).cluster().state(ACTIVE);

        grid(0).createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setAffinity(new GridCacheModuloAffinityFunction(3, 2)));

        for (SecurityCredentials cred : TEST_CREDENTIALS) {
            SecurityContext secCtx = authenticate(grid(0), DFAULT_USER_NAME, "ignite");

            try (AutoCloseable ignored = grid(0).context().security().withContext(secCtx)) {
                grid(0).context().security().createUser(
                    (String)cred.getLogin(),
                    ((String)cred.getPassword()).toCharArray()
                );
            }

            checkMultiNodeClientOperation(cred, 2);
        }

        stopAllGrids();

        startGrid(0);

        startNodeProcess(1, "UTF-8");
        startNodeProcess(2, "Big5");

        grid(0).cluster().state(ACTIVE);

        for (SecurityCredentials cred : TEST_CREDENTIALS)
            checkMultiNodeClientOperation(cred, 2);
    }

    /** */
    private void checkMultiNodeClientOperation(SecurityCredentials cred, int nodesCnt) {
        for (int nodeIdx = 0; nodeIdx < nodesCnt; ++nodeIdx) {
            try (IgniteClient cli = connectClient(nodeIdx, cred)) {
                ClientCache<Object, Object> cache = cli.cache(DEFAULT_CACHE_NAME);

                for (int i = 0; i < nodesCnt; i++) {
                    cache.put(i, i);

                    assertEquals(i, cache.get(i));
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean isRemoteJvm(String igniteInstanceName) {
        return getTestIgniteInstanceIndex(igniteInstanceName) != 0;
    }

    /** */
    @Override protected List<String> additionalRemoteJvmArgs() {
        return Arrays.asList(
            "-Dfile.encoding=" + remoteJvmEncoding,
            "-D" + IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2 + "=true"
        );
    }

    /** */
    private IgniteEx startNodeProcess(int idx, String encoding) throws Exception {
        remoteJvmEncoding = encoding;

        return startGrid(idx);
    }

    /** */
    private static IgniteClient connectClient(int nodeIdx, SecurityCredentials creds) {
        return Ignition.startClient(new ClientConfiguration()
            .setAddresses("127.0.0.1:1080" + nodeIdx)
            .setUserName((String)creds.getLogin())
            .setUserPassword((String)creds.getPassword()));
    }
}
