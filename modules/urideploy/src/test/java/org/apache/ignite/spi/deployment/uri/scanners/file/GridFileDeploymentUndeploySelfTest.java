/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.deployment.uri.scanners.file;

import org.apache.ignite.spi.deployment.uri.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.spi.*;
import java.io.*;
import java.util.*;

/**
 * Tests correct task undeployment after source file removing.
 */
@GridSpiTest(spi = GridUriDeploymentSpi.class, group = "Deployment SPI")
public class GridFileDeploymentUndeploySelfTest extends GridSpiAbstractTest<GridUriDeploymentSpi> {
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
            U.resolveGridGainPath(GridTestProperties.getProperty("ant.urideployment.gar.file")).getPath();

        File garFile = new File(garFilePath);

        assert garFile.exists() : "Test gar file not found [path=" + garFilePath + ']';

        File newGarFile = new File(tmpDirPath + '/' + GAR_FILE_NAME);

        assert !newGarFile.exists();

        U.copy(garFile, newGarFile, false);

        assert newGarFile.exists();

        Thread.sleep(GridUriDeploymentSpi.DFLT_DISK_SCAN_FREQUENCY + 3000);

        assert getSpi().findResource("org.gridgain.grid.spi.deployment.uri.tasks.GridUriDeploymentTestTask3") != null :
            "Failed to find resource for added GAR file.";
        assert getSpi().findResource("GridUriDeploymentTestWithNameTask3") != null;

        boolean del = newGarFile.delete();

        assert del : "Filed to delete file [path=" + newGarFile.getAbsolutePath() + ']';

        assert !newGarFile.exists();

        Thread.sleep(GridUriDeploymentSpi.DFLT_DISK_SCAN_FREQUENCY + 3000);

        assert getSpi().findResource("org.gridgain.grid.spi.deployment.uri.tasks.GridUriDeploymentTestTask3") == null;
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
