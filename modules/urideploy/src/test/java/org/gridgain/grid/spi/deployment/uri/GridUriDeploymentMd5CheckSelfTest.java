/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.deployment.uri;

import org.gridgain.grid.spi.deployment.*;
import org.gridgain.grid.util.typedef.internal.U;
import org.gridgain.testframework.config.GridTestProperties;
import org.gridgain.testframework.junits.spi.*;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Redundancy for URI deployment test
 */
@GridSpiTest(spi = GridUriDeploymentSpi.class, group = "Deployment SPI")
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
