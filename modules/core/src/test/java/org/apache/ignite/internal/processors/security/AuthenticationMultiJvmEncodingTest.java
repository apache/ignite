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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.authentication.AuthenticationProcessorSelfTest.authenticate;
import static org.apache.ignite.internal.processors.authentication.AuthenticationProcessorSelfTest.withSecurityContextOnAllNodes;

/** Tests that authenticator is independent of default JVM characters encoding. */
public class AuthenticationMultiJvmEncodingTest extends GridCommonAbstractTest {
    /** Character encoding name that will be applied to the next Ignite process start. */
    private String jvmCharacterEncoding;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setAuthenticationEnabled(true);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(100 * (1 << 20))
                .setPersistenceEnabled(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected List<String> additionalRemoteJvmArgs() {
        List<String> args = super.additionalRemoteJvmArgs();

        args.add("-Dfile.encoding=" + jvmCharacterEncoding);

        return args;
    }

    /** {@inheritDoc} */
    @Override protected boolean isRemoteJvm(String igniteInstanceName) {
        return getTestIgniteInstanceIndex(igniteInstanceName) != 0;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /**
     *  Tests scenario when Ignite cluster nodes run inside JVMs with different default charset encoding and user
     *  login has different representation across all of them.
     */
    @Test
    public void testMultipleJvmInstancesWithDifferentEncoding() throws Exception {
        startGrid(0);

        jvmCharacterEncoding = "Big5";

        startGrid(1);

        jvmCharacterEncoding = "UTF-8";

        startGrid(2);

        grid(0).cluster().state(ACTIVE);

        grid(0).createCache(DEFAULT_CACHE_NAME);

        // Here we use symbols that have different representation on started above nodes.
        String login = "語";
        String pwd = "語";

        try (AutoCloseable ignored = withSecurityContextOnAllNodes(authenticate(grid(0), "ignite", "ignite"))) {
            grid(0).context().security().createUser(login, pwd.toCharArray());
        }

        try (
            IgniteClient cli = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800")
                .setUserName(login)
                .setUserPassword(pwd))
        ) {
            ClientCache<Integer, Integer> cache = cli.cache(DEFAULT_CACHE_NAME);

            Map<Integer, Integer> entries = new HashMap<>();

            for (int i = 0; i < 1000; i++)
                entries.put(i, i);

            cache.putAll(entries);
        }
    }
}
