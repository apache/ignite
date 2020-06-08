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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.NoopConsole;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandList.DEACTIVATE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePassword;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePath;
import static org.apache.ignite.testframework.GridTestUtils.sslTrustedFactory;

/**
 * Command line handler test with SSL and security.
 */
public class GridCommandHandlerSslWithSecurityTest extends GridCommonAbstractTest {
    /** Login. */
    private final String login = "testUsr";

    /** Password. */
    private final String pwd = "testPwd";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setPluginProviders(new TestSecurityPluginProvider(login, pwd, ALLOW_ALL, null, false))
            .setSslContextFactory(sslTrustedFactory("node01", "trustone"))
            .setConnectorConfiguration(new ConnectorConfiguration().setSslEnabled(true));
    }

    /**
     * Verify that the command work correctly when entering passwords for
     * keystore and truststore, and that these passwords are requested only
     * once.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testInputKeyTrustStorePwdOnlyOnce() throws Exception {
        IgniteEx crd = startGrid();

        crd.cluster().active(true);

        CommandHandler cmd = new CommandHandler();

        AtomicInteger keyStorePwdCnt = new AtomicInteger();
        AtomicInteger trustStorePwdCnt = new AtomicInteger();

        cmd.console = new NoopConsole() {
            /** {@inheritDoc} */
            @Override public char[] readPassword(String fmt, Object... args) {
                if (fmt.contains("keystore")) {
                    keyStorePwdCnt.incrementAndGet();

                    return keyStorePassword().toCharArray();
                }
                else if (fmt.contains("truststore")) {
                    trustStorePwdCnt.incrementAndGet();

                    return keyStorePassword().toCharArray();
                }

                return pwd.toCharArray();
            }
        };

        List<String> args = new ArrayList<>();

        args.add(DEACTIVATE.text());
        args.add("--force");
        args.add("--yes");

        args.add("--user");
        args.add(login);

        args.add("--keystore");
        args.add(keyStorePath("node01"));

        args.add("--truststore");
        args.add(keyStorePath("trustone"));

        assertEquals(EXIT_CODE_OK, cmd.execute(args));
        assertEquals(1, keyStorePwdCnt.get());
        assertEquals(1, trustStorePwdCnt.get());
    }
}
