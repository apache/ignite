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
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.lang.GridAbsPredicateX;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTestConfig;
import org.apache.ignite.util.antgar.IgniteDeploymentGarAntTask;
import org.apache.tools.ant.Project;

/**
 *
 */
@GridSpiTest(spi = UriDeploymentSpi.class, group = "Deployment SPI")
public class GridUriDeploymentFileProcessorSelfTest extends GridUriDeploymentAbstractSelfTest {
    /**
     * @throws Exception If failed.
     */
    public void testTaskCorrect() throws Exception {
        proceedTest("correct.gar", "ignite.xml",
            "org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask0", true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTaskWithBrokenXML() throws Exception {
        proceedTest("broken.gar", "ignite.brokenxml", "brokenxml-task", false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTaskWithEmptyXML() throws Exception {
        proceedTest("empty.gar", "ignite.empty", "emptyxml-task", false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTaskWithIncorrectRefsXML() throws Exception {
        proceedTest("incorrefs.gar", "ignite.incorrefs", "incorrectref-task", false);
    }

    /**
     * @param garFileName Name of .gar file.
     * @param garDescFileName Name of Ignite descriptor file.
     * @param taskId Task id.
     * @param deployed If {@code true} then givent task should be deployed after test,
     *      if {@code false} then it should be undeployed.
     * @throws Exception If failed.
     */
    private void proceedTest(String garFileName, String garDescFileName, final String taskId, final boolean deployed)
        throws Exception {
        info("This test checks broken tasks. All exceptions that might happen are the part of the test.");

        String tmpDirName = GridTestProperties.getProperty("ant.gar.tmpdir");
        String srcDirName = GridTestProperties.getProperty("ant.gar.srcdir");
        String baseDirName = tmpDirName + File.separator + System.currentTimeMillis();
        String metaDirName = baseDirName + File.separator + "META-INF";
        String garDescDirName =
            U.resolveIgnitePath(GridTestProperties.getProperty("deploy.gar.descriptor.dir")) +
            File.separator + garDescFileName;

        // Make base, META-INF and deployment dirs.
        File destDir = new File(GridTestProperties.getProperty("deploy.uri.file2.path"));

        if (!destDir.exists()) {
            boolean mkdir = destDir.mkdirs();

            assert mkdir;
        }

        boolean mkdir = new File(baseDirName).mkdirs();

        assert mkdir;

        mkdir = new File(metaDirName).mkdirs();

        assert mkdir;

        // Make Gar file
        U.copy( new File(garDescDirName), new File(metaDirName + File.separator + "ignite.xml"), true);

        // Copy files to basedir
        U.copy(new File(srcDirName), new File(baseDirName), true);

        File garFile = new File(baseDirName + File.separator + garFileName);

        IgniteDeploymentGarAntTask garTask = new IgniteDeploymentGarAntTask();

        Project garProject = new Project();

        garProject.setName("Gar test project");

        garTask.setDestFile(garFile);
        garTask.setBasedir(new File(baseDirName));
        garTask.setProject(garProject);

        garTask.execute();

        assert garFile.exists();

        // Copy to deployment directory.
        U.copy(garFile, destDir, true);

        try {
            // Wait for SPI
            GridTestUtils.waitForCondition(new GridAbsPredicateX() {
                @Override public boolean applyx() throws IgniteCheckedException {
                    if (deployed)
                        return getSpi().findResource(taskId) != null;
                    else
                        return getSpi().findResource(taskId) == null;
                }
            }, 5000);

            if (deployed)
                assert getSpi().findResource(taskId) != null;
            else
                assert getSpi().findResource(taskId) == null;
        }
        finally {
            U.delete(destDir);

            // Wait for SPI refresh
            Thread.sleep(1000);
        }
    }

    /**
     * @return List of URI to be used as deployment source.
     */
    @GridSpiTestConfig
    public List<String> getUriList() {
        File destDir = new File(GridTestProperties.getProperty("deploy.uri.file2.path"));

        if (!destDir.exists()) {
            boolean mkdir = destDir.mkdirs();

            assert mkdir;
        }

        List<String> uriList = new ArrayList<>();

        uriList.add(GridTestProperties.getProperty("deploy.uri.file2"));

        return uriList;
    }
}
