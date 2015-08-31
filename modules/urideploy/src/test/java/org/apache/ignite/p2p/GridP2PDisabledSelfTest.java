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

package org.apache.ignite.p2p;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.deployment.uri.UriDeploymentSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Test what happens if peer class loading is disabled.
 * <p>
 * In order for this test to run, make sure that your
 * {@code p2p.uri.cls} folder ends with {@code .gar} extension.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared", "ProhibitedExceptionThrown"})
@GridCommonTest(group = "P2P")
public class GridP2PDisabledSelfTest extends GridCommonAbstractTest {
    /** Task name. */
    private static final String TASK_NAME = "org.apache.ignite.tests.p2p.P2PTestTaskExternalPath1";

    /** External class loader. */
    private static ClassLoader extLdr;

    /** Current deployment mode. Used in {@link #getConfiguration(String)}. */
    private DeploymentMode depMode;

    /** */
    private boolean initGar;

    /** Path to GAR file. */
    private String garFile;

    /**
     *
     */
    public GridP2PDisabledSelfTest() {
        super(/*start grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        extLdr = getExternalClassLoader();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(false);

        cfg.setDeploymentMode(depMode);

        if (initGar) {
            UriDeploymentSpi depSpi = new UriDeploymentSpi();

            depSpi.setUriList(Collections.singletonList(garFile));

            cfg.setDeploymentSpi(depSpi);
        }

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setHeartbeatFrequency(500);

        return cfg;
    }

    /**
     * Test what happens if peer class loading is disabled.
     *
     * @throws Exception if error occur.
     */
    @SuppressWarnings("unchecked")
    private void checkClassNotFound() throws Exception {
        initGar = false;

        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            Class task = extLdr.loadClass(TASK_NAME);

            try {
                ignite1.compute().execute(task, ignite2.cluster().localNode().id());

                assert false;
            }
            catch (IgniteException e) {
                info("Received expected exception: " + e);
            }
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /**
     * @throws Exception if error occur.
     */
    @SuppressWarnings("unchecked")
    private void checkGar() throws Exception {
        initGar = true;

        String garDir = "modules/extdata/p2p/target/deploy";
        String garFileName = "p2p.gar";

        File origGarPath = U.resolveIgnitePath(garDir + '/' + garFileName);

        File tmpPath = new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());

        if (!tmpPath.mkdir())
            throw new IOException("Can not create temp directory");

        try {
            File newGarFile = new File(tmpPath, garFileName);

            U.copy(origGarPath, newGarFile, false);

            assert newGarFile.exists();

            try {
                garFile = "file:///" + tmpPath.getAbsolutePath();

                try {
                    Ignite ignite1 = startGrid(1);
                    Ignite ignite2 = startGrid(2);

                    Integer res = ignite1.compute().<UUID, Integer>execute(TASK_NAME, ignite2.cluster().localNode().id());

                    assert res != null;
                }
                finally {
                    stopGrid(1);
                    stopGrid(2);
                }
            }
            finally {
                if (newGarFile != null && !newGarFile.delete())
                    error("Can not delete temp gar file");
            }
        }
        finally {
            if (!tmpPath.delete())
                error("Can not delete temp directory");
        }
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testGarPrivateMode() throws Exception {
        depMode = DeploymentMode.PRIVATE;

        checkGar();
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testGarIsolatedMode() throws Exception {
        depMode = DeploymentMode.ISOLATED;

        checkGar();
    }

    /**
     * Test GridDeploymentMode.CONTINUOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testGarContinuousMode() throws Exception {
        depMode = DeploymentMode.CONTINUOUS;

        checkGar();
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testGarSharedMode() throws Exception {
        depMode = DeploymentMode.SHARED;

        checkGar();
    }
}