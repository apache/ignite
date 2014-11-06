/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.deployment.uri;

import org.apache.tools.ant.*;
import org.gridgain.grid.util.antgar.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.spi.*;
import java.io.*;
import java.util.*;

/**
 *
 */
@GridSpiTest(spi = GridUriDeploymentSpi.class, group = "Deployment SPI")
public class GridUriDeploymentFileProcessorSelfTest extends GridUriDeploymentAbstractSelfTest {
    /**
     * @throws Exception If failed.
     */
    public void testTaskCorrect() throws Exception {
        proceedTest("correct.gar", "gridgain.xml",
            "org.gridgain.grid.spi.deployment.uri.tasks.GridUriDeploymentTestTask0", true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTaskWithBrokenXML() throws Exception {
        proceedTest("broken.gar", "gridgain.brokenxml", "brokenxml-task", false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTaskWithEmptyXML() throws Exception {
        proceedTest("empty.gar", "gridgain.empty", "emptyxml-task", false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTaskWithIncorrectRefsXML() throws Exception {
        proceedTest("incorrefs.gar", "gridgain.incorrefs", "incorrectref-task", false);
    }

    /**
     * @param garFileName Name of .gar file.
     * @param garDescFileName Name of GridGain descriptor file.
     * @param taskId Task id.
     * @param deployed If {@code true} then givent task should be deployed after test,
     *      if {@code false} then it should be undeployed.
     * @throws Exception If failed.
     */
    private void proceedTest(String garFileName, String garDescFileName, String taskId, boolean deployed)
        throws Exception {
        info("This test checks broken tasks. All exceptions that might happen are the part of the test.");

        String tmpDirName = GridTestProperties.getProperty("ant.gar.tmpdir");
        String srcDirName = GridTestProperties.getProperty("ant.gar.srcdir");
        String baseDirName = tmpDirName + File.separator + System.currentTimeMillis();
        String metaDirName = baseDirName + File.separator + "META-INF";
        String garDescDirName =
            U.resolveGridGainPath(GridTestProperties.getProperty("deploy.gar.descriptor.dir")) +
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
        U.copy( new File(garDescDirName), new File(metaDirName + File.separator + "gridgain.xml"), true);

        // Copy files to basedir
        U.copy(new File(srcDirName), new File(baseDirName), true);

        File garFile = new File(baseDirName + File.separator + garFileName);

        GridDeploymentGarAntTask garTask = new GridDeploymentGarAntTask();

        Project garProject = new Project();

        garProject.setName("Gar test project");

        garTask.setDestFile(garFile);
        garTask.setBasedir(new File(baseDirName));
        garTask.setProject(garProject);

        garTask.execute();

        assert garFile.exists();

        // Copy to deployment directory.
        U.copy(garFile, destDir, true);

        // Wait for SPI
        Thread.sleep(1000);

        try {
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
