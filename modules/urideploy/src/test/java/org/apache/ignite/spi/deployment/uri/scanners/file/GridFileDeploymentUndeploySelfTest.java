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

package org.apache.ignite.spi.deployment.uri.scanners.file;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.deployment.uri.UriDeploymentSpi;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTestConfig;

/**
 * Tests correct task undeployment after source file removing.
 */
@GridSpiTest(spi = UriDeploymentSpi.class, group = "Deployment SPI")
public class GridFileDeploymentUndeploySelfTest extends GridSpiAbstractTest<UriDeploymentSpi> {
    /** */
    private static String tmpDirPath = System.getProperty("java.io.tmpdir") + '/' + UUID.randomUUID();

    /** */
    private static final String GAR_FILE_NAME = "deployfile.gar";

    /** {@inheritDoc} */
    @Override protected void beforeSpiStarted() throws Exception {
        File deployDir = new File(tmpDirPath);

        assert !deployDir.exists() : "Directory exists " + deployDir.getCanonicalPath();

        boolean mkdir = deployDir.mkdir();

        assert mkdir : "Unable to create directory: " + deployDir.getCanonicalPath();

        info("URI list for test [uriList=" + getUriList() + ']');

        super.beforeSpiStarted();
    }

    /**
     * @throws Exception If failed.
     */
    public void testUndeployGarFile() throws Exception {
        String garFilePath =
            U.resolveIgnitePath(GridTestProperties.getProperty("ant.urideployment.gar.file")).getPath();

        File garFile = new File(garFilePath);

        assert garFile.exists() : "Test gar file not found [path=" + garFilePath + ']';

        File newGarFile = new File(tmpDirPath + '/' + GAR_FILE_NAME);

        assert !newGarFile.exists();

        U.copy(garFile, newGarFile, false);

        assert newGarFile.exists();

        Thread.sleep(UriDeploymentFileScanner.DFLT_SCAN_FREQ + 3000);

        assert getSpi().findResource("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask3") != null :
            "Failed to find resource for added GAR file.";
        assert getSpi().findResource("GridUriDeploymentTestWithNameTask3") != null;

        boolean del = newGarFile.delete();

        assert del : "Filed to delete file [path=" + newGarFile.getAbsolutePath() + ']';

        assert !newGarFile.exists();

        Thread.sleep(UriDeploymentFileScanner.DFLT_SCAN_FREQ + 3000);

        assert getSpi().findResource("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask3") == null;
        assert getSpi().findResource("GridUriDeploymentTestWithNameTask3") == null;
    }

    /**
     * @throws Exception If failed.
     */
    @Override protected void afterTestsStopped() throws Exception {
        File deployDir = new File(tmpDirPath);

        if (deployDir.exists())
            deployDir.delete();
    }

    /**
     * @return List of URI to use as deployment source.
     */
    @GridSpiTestConfig
    public List<String> getUriList() {
        return Collections.singletonList("file:///" + tmpDirPath);
    }
}