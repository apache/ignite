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

package org.apache.ignite.compatibility.clients;

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.compatibility.testframework.junits.Dependency;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityNodeRunner;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.compatibility.IgniteReleasedVersion.VER_2_4_0;
import static org.apache.ignite.compatibility.IgniteReleasedVersion.since;
import static org.apache.ignite.testframework.GridTestUtils.cartesianProduct;

/**
 * Tests that current client version can connect to the server with specified version and
 * specified client version can connect to the current server version.
 */
@RunWith(Parameterized.class)
public abstract class AbstractClientCompatibilityTest extends IgniteCompatibilityAbstractTest {
    /** Version 2.5.0. */
    protected static final IgniteProductVersion VER_2_5_0 = IgniteProductVersion.fromString("2.5.0");

    /** Version 2.7.0. */
    protected static final IgniteProductVersion VER_2_7_0 = IgniteProductVersion.fromString("2.7.0");

    /** Version 2.8.0. */
    protected static final IgniteProductVersion VER_2_8_0 = IgniteProductVersion.fromString("2.8.0");

    /** Version 2.9.0. */
    protected static final IgniteProductVersion VER_2_9_0 = IgniteProductVersion.fromString("2.9.0");

    /** Version 2.10.0. */
    protected static final IgniteProductVersion VER_2_10_0 = IgniteProductVersion.fromString("2.10.0");

    /** Version 2.11.0. */
    protected static final IgniteProductVersion VER_2_11_0 = IgniteProductVersion.fromString("2.11.0");

    /** Version 2.12.0. */
    protected static final IgniteProductVersion VER_2_12_0 = IgniteProductVersion.fromString("2.12.0");

    /** Version 2.13.0. */
    protected static final IgniteProductVersion VER_2_13_0 = IgniteProductVersion.fromString("2.13.0");

    /** Version 2.14.0. */
    protected static final IgniteProductVersion VER_2_14_0 = IgniteProductVersion.fromString("2.14.0");

    /** Version 2.15.0. */
    protected static final IgniteProductVersion VER_2_15_0 = IgniteProductVersion.fromString("2.15.0");

    /** Version 2.18.0. */
    protected static final IgniteProductVersion VER_2_18_0 = IgniteProductVersion.fromString("2.18.0");

    /** Parameters. */
    @Parameterized.Parameters(name = "Version {0}")
    public static Iterable<Object[]> versions() {
        return cartesianProduct(F.concat(true, IgniteVersionUtils.VER_STR, since(VER_2_4_0)));
    }

    /** Old Ignite version. */
    @Parameterized.Parameter
    public String verFormatted;

    /** */
    protected IgniteProductVersion ver;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        ver = IgniteProductVersion.fromString(verFormatted);
    }

    /** {@inheritDoc} */
    @Override protected @NotNull Collection<Dependency> getDependencies(String igniteVer) {
        Collection<Dependency> dependencies = super.getDependencies(igniteVer);

        dependencies.add(new Dependency("indexing", "ignite-indexing", false));

        // Add corresponding H2 version.
        if (ver.compareTo(VER_2_7_0) < 0)
            dependencies.add(new Dependency("h2", "com.h2database", "h2", "1.4.195", false));

        return dependencies;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOldClientToCurrentServer() throws Exception {
        try (Ignite ignite = startGrid(0)) {
            initNode(ignite);

            if (verFormatted.equals(IgniteVersionUtils.VER_STR))
                testClient(verFormatted);
            else {
                String fileName = IgniteCompatibilityNodeRunner.storeToFile((IgniteInClosure<String>)this::testClient);

                GridJavaProcess proc = GridJavaProcess.exec(
                    RemoteClientRunner.class.getName(),
                    IgniteVersionUtils.VER_STR + ' ' + fileName,
                    log,
                    log::info,
                    null,
                    null,
                    getProcessProxyJvmArgs(verFormatted),
                    null
                );

                try {
                    GridTestUtils.waitForCondition(() -> !proc.getProcess().isAlive(), GridTestUtils.DFLT_TEST_TIMEOUT);

                    assertEquals(0, proc.getProcess().exitValue());
                }
                finally {
                    if (proc.getProcess().isAlive())
                        proc.kill();
                }
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCurrentClientToOldServer() throws Exception {
        try {
            if (verFormatted.equals(IgniteVersionUtils.VER_STR)) {
                Ignite ignite = startGrid(0);

                initNode(ignite);
            }
            else
                startGrid(1, verFormatted, this::processRemoteConfiguration, this::initNode);

            testClient(verFormatted);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Method to initiate server node (node can be local or remote).
     *
     * @param ignite Ignite.
     */
    protected void initNode(Ignite ignite) {
        // No-op.
    }

    /**
     * Method to change remote server node configuration.
     *
     * @param cfg Ignite configuraion.
     */
    protected void processRemoteConfiguration(IgniteConfiguration cfg) {
        cfg.setLocalHost("127.0.0.1");
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(new TcpDiscoveryVmIpFinder(true)));
    }

    /**
     * Method to test client operations (client can be local or remote).
     *
     * @param clientVer Client version.
     * @param serverVer Server version.
     */
    protected abstract void testClient(IgniteProductVersion clientVer, IgniteProductVersion serverVer)
        throws Exception;

    /**
     * @param serverVer Server version.
     */
    private void testClient(String serverVer) {
        try {
            IgniteProductVersion clientVer = IgniteVersionUtils.VER;

            X.println(">>> Started client test [clientVer=" + clientVer + ", serverVer=" + serverVer + ']');

            testClient(clientVer, IgniteProductVersion.fromString(serverVer));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Runner class to test client operations from remote JVM process with old Ignite version
     * as dependencies in class path.
     */
    public static class RemoteClientRunner {
        /** */
        public static void main(String[] args) throws Exception {
            X.println(GridJavaProcess.PID_MSG_PREFIX + U.jvmPid());
            X.println("Start client connection with Ignite version: " + IgniteVersionUtils.VER);

            if (args.length < 2)
                throw new IllegalArgumentException("At least 2 arguments expected: [version] [path/to/closure/file]");

            String ver = args[0];
            String fileName = args[1];

            IgniteInClosure<String> clo = IgniteCompatibilityNodeRunner.readClosureFromFileAndDelete(fileName);

            clo.apply(ver);

            X.println("Success");
        }
    }
}
