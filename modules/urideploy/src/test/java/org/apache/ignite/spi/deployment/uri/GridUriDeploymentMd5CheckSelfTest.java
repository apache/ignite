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

package org.apache.ignite.spi.deployment.uri;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.deployment.DeploymentListener;
import org.apache.ignite.spi.deployment.DeploymentResource;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTestConfig;

/**
 * Redundancy for URI deployment test
 */
@GridSpiTest(spi = UriDeploymentSpi.class, group = "Deployment SPI")
public class GridUriDeploymentMd5CheckSelfTest extends GridUriDeploymentAbstractSelfTest {
    /**
     * Used to count number of unit undeployments.
     */
    private AtomicInteger undeployCntr = new AtomicInteger();

    /**
     * Test skipping fresh deployment of duplicated .gar files.
     *
     * @throws Exception if failed.
     */
    public void testMd5FileCheck() throws Exception {
        undeployCntr.set(0);

        DeploymentResource task = getSpi().findResource("GridUriDeploymentTestWithNameTask7");

        assert task == null;

        U.copy(getGarFile(), new File(getDeployDir(), "uri1.gar"), true);

        Thread.sleep(500);

        task = getSpi().findResource("GridUriDeploymentTestWithNameTask7");

        assert task != null;
        assert undeployCntr.get() == 0;

        U.copy(getGarFile(), new File(getDeployDir(), "uri2.gar"), true);

        Thread.sleep(500);

        task = getSpi().findResource("GridUriDeploymentTestWithNameTask7");

        assert task != null;
        assert undeployCntr.get() == 0;
    }

    /**
     * Test skipping fresh deployment of .gar directories with equal content.
     *
     * @throws Exception if failed.
     */
    public void testMd5DirectoryCheck() throws Exception {
        undeployCntr.set(0);

        DeploymentResource task = getSpi().findResource("GridUriDeploymentTestWithNameTask6");

        assert task == null;

        U.copy(getGarDir(), new File(getDeployDir(), "uri1.gar"), true);

        Thread.sleep(500);

        task = getSpi().findResource("GridUriDeploymentTestWithNameTask6");

        assert task != null;
        assert undeployCntr.get() == 0;

        U.copy(getGarDir(), new File(getDeployDir(), "uri2.gar"), true);

        Thread.sleep(500);

        task = getSpi().findResource("GridUriDeploymentTestWithNameTask6");

        assert task != null;
        assert undeployCntr.get() == 0;

        U.delete(getGarDir());
        U.delete(new File(getDeployDir(), "uri1.gar"));
        U.delete(new File(getDeployDir(), "uri2.gar"));
    }

    /**
     * Prepares and returns a directory for test deployments.
     *
     * @return directory used as deployment source in this test.
     */
    private File getDeployDir() {
        File tmpDir = new File(GridTestProperties.getProperty("deploy.uri.file2.path"));

        if (! tmpDir.exists())
            tmpDir.mkdirs();

        assert tmpDir.isDirectory();

        return tmpDir;
    }

    /**
     * Returns original .gar file to use in this test.
     *
     * @return a valid .gar file path.
     */
    private File getGarFile() {
        File gar = new File(GridTestProperties.getProperty("ant.urideployment.gar.file"));

        assert gar.isFile();

        return gar;
    }

    /**
     * Prepares and returns .gar directory to use in this test.
     *
     * @return a valid .gar directory.
     * @throws IOException if such directory can't be created.
     */
    private File getGarDir() throws IOException {
        File file = getGarFile();
        File parent = file.getParentFile();

        assert parent.isDirectory();

        File garDir = new File(parent, "extracted_" + file.getName());

        // If content wasn't extracted before
        if (!garDir.isDirectory()) {
            garDir.mkdirs();
            U.unzip(file, garDir, null);
        }

        return garDir;
    }

    /**
     * @return List of URI to use as deployment source.
     */
    @GridSpiTestConfig
    public List<String> getUriList() {
        getDeployDir(); // Force creation.
        return Collections.singletonList(GridTestProperties.getProperty("deploy.uri.file2"));
    }

    /**
     * @return {@code true}
     */
    @GridSpiTestConfig
    public boolean getCheckMd5() {
        return true;
    }

    /**
     * Sets listener to increment {@code undeployCounter}
     *
     * @throws Exception if failed.
     */
    @Override
    protected void beforeTestsStarted() throws Exception {
        getSpi().setListener(new DeploymentListener() {
            @Override
            public void onUnregistered(ClassLoader ldr) {
                undeployCntr.incrementAndGet();
            }
        });
    }

    /**
     * Cleans temporary deployment directory.
     *
     * @throws Exception if cleanup failed.
     */
    @Override
    protected void afterTestsStopped() throws Exception {
        U.delete(getDeployDir());
    }
}