/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.deployment.uri;

import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.net.*;
import java.util.concurrent.*;

/**
 * Grid URI deployment class loader self test.
 */
public class GridUriDeploymentClassLoaderMultiThreadedSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testMultiThreadedClassLoading() throws Exception {
        for (int i = 0; i < 50; i++)
            doTest();
    }

    /**
     * @throws Exception If failed.
     */
    private void doTest() throws Exception {
        final GridUriDeploymentClassLoader ldr = new GridUriDeploymentClassLoader(
            new URL[] { U.resolveGridGainUrl(GridTestProperties.getProperty("ant.urideployment.gar.file")) },
                getClass().getClassLoader());

        multithreaded(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    ldr.loadClass("org.gridgain.grid.spi.deployment.uri.tasks.GridUriDeploymentTestTask0");

                    return null;
                }
            },
            500
        );

        final GridUriDeploymentClassLoader ldr0 = new GridUriDeploymentClassLoader(
            new URL[] { U.resolveGridGainUrl(GridTestProperties.getProperty("ant.urideployment.gar.file")) },
            getClass().getClassLoader());

        multithreaded(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    ldr0.loadClassGarOnly("org.gridgain.grid.spi.deployment.uri.tasks.GridUriDeploymentTestTask0");

                    return null;
                }
            },
            500
        );
    }
}
