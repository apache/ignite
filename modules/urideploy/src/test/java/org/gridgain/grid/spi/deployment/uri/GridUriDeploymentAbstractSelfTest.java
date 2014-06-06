/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.deployment.uri;

import org.gridgain.grid.spi.deployment.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.spi.*;

/**
 *
 */
public abstract class GridUriDeploymentAbstractSelfTest extends GridSpiAbstractTest<GridUriDeploymentSpi> {
    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        getSpi().setListener(null);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        getSpi().setListener(new GridDeploymentListener() {
            @Override public void onUnregistered(ClassLoader ldr) {
                // No-op.
            }
        });
    }

    /**
     * @return Temporary directory to be used in test.
     */
    @GridSpiTestConfig
    public String getTemporaryDirectoryPath() {
        String path = GridTestProperties.getProperty("deploy.uri.tmpdir");

        assert path != null;

        return path;
    }

    /**
     * @param taskName Name of available task.
     * @throws Exception if failed.
     */
    protected void checkTask(String taskName) throws Exception {
        assert taskName != null;

        GridDeploymentResource task = getSpi().findResource(taskName);

        assert task != null;

        info("Deployed task [task=" + task + ']');
    }

    /**
     * @param taskName name of unavailable task.
     * @throws Exception if failed.
     */
    protected void checkNoTask(String taskName) throws Exception {
        assert taskName != null;

        GridDeploymentResource task = getSpi().findResource(taskName);

        assert task == null;

        info("Not deployed task [task=" + task + ']');
    }
}
