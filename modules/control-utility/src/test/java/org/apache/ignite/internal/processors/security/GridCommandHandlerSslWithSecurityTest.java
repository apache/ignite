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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.NoopConsole;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.util.GridCommandHandlerFactoryAbstractTest;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALL_PERMISSIONS;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePassword;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePath;
import static org.apache.ignite.testframework.GridTestUtils.sslTrustedFactory;

/**
 * Command line handler test with SSL and security.
 */
public class GridCommandHandlerSslWithSecurityTest extends GridCommandHandlerFactoryAbstractTest {
    /** Login. */
    private final String login = "testUsr";

    /** Password. */
    private final String pwd = "testPwd";

    /** System out. */
    protected static PrintStream sysOut;

    /**
     * Test out - can be injected via {@link #injectTestSystemOut()} instead of System.out and analyzed in test.
     * Will be as well passed as a handler output for an anonymous logger in the test.
     */
    protected static ByteArrayOutputStream testOut;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        testOut = new ByteArrayOutputStream(16 * 1024);

        sysOut = System.out;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Assume.assumeTrue(cliCommandHandler());

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        System.setOut(sysOut);

        testOut.reset();

        stopAllGrids();
    }

    /**
     * Sets test output stream.
     */
    protected void injectTestSystemOut() {
        System.setOut(new PrintStream(testOut));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName)
            .setPluginProviders(new TestSecurityPluginProvider(login, pwd, ALL_PERMISSIONS, null, false))
            .setSslContextFactory(sslTrustedFactory("node01", "trustone"));

        if (commandHandler.equals(CLI_CMD_HND)) {
            cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration()
                .setSslEnabled(true)
                .setSslContextFactory(sslTrustedFactory("thinServer", "trusttwo"))
                .setUseIgniteSslContextFactory(false)
            );
        }

        return cfg;
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

        crd.cluster().state(ACTIVE);

        TestCommandHandler cmd = newCommandHandler();

        AtomicInteger keyStorePwdCnt = new AtomicInteger();
        AtomicInteger trustStorePwdCnt = new AtomicInteger();

        ((CommandHandler)GridTestUtils.getFieldValue(cmd, "hnd")).console = new NoopConsole() {
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

        args.add("--deactivate");
        args.add("--force");
        args.add("--yes");

        args.add("--user");
        args.add(login);

        args.add("--keystore");
        args.add(keyStorePath(CLI_CMD_HND.equals(commandHandler) ? "thinClient" : "connectorServer"));

        args.add("--truststore");
        args.add(keyStorePath(CLI_CMD_HND.equals(commandHandler) ? "trusttwo" : "trustthree"));

        assertEquals(EXIT_CODE_OK, cmd.execute(args));
        assertEquals(1, keyStorePwdCnt.get());
        assertEquals(1, trustStorePwdCnt.get());
    }

    /**
     * Checks that control.sh script can connect to the cluster, that has SSL enabled.
     */
    @Test
    public void testConnector() throws Exception {
        IgniteEx crd = startGrid();

        crd.cluster().state(ACTIVE);

        injectTestSystemOut();

        TestCommandHandler hnd = newCommandHandler();

        int exitCode = hnd.execute(Arrays.asList(
            "--state",
            "--user", login,
            "--password", pwd,
            "--keystore", keyStorePath(CLI_CMD_HND.equals(commandHandler) ? "thinClient" : "connectorServer"),
            "--keystore-password", keyStorePassword(),
            "--truststore", keyStorePath(CLI_CMD_HND.equals(commandHandler) ? "trusttwo" : "trustthree"),
            "--truststore-password", keyStorePassword()));

        assertEquals(EXIT_CODE_OK, exitCode);

        hnd.flushLogger();

        // Make sure all sensitive information is masked.
        String testOutput = testOut.toString();
        assertContains(log, testOutput, "--password *****");
        assertContains(log, testOutput, "--keystore-password *****");
        assertContains(log, testOutput, "--truststore-password *****");
    }
}
