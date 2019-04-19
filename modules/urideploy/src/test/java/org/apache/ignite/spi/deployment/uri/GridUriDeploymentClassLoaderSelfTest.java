/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.deployment.uri;

import java.net.URL;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.deployment.DeploymentResource;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTestConfig;
import org.junit.Test;

/**
 * Grid URI deployment class loader test.
 */
@GridSpiTest(spi = UriDeploymentSpi.class, group = "Deployment SPI")
public class GridUriDeploymentClassLoaderSelfTest extends GridUriDeploymentAbstractSelfTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNestedJarClassloading() throws Exception {
        ClassLoader ldr = getGarClassLoader();

        // Load class from nested JAR file
        assert ldr.loadClass("javax.mail.Service") != null;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClasspathResourceLoading() throws Exception {
        ClassLoader ldr = getGarClassLoader();

        // Get resource from GAR file
        URL rsrcUrl = ldr.getResource("org/apache/ignite/test/test.properties");

        assert rsrcUrl != null;
    }

    /**
     * @return Test GAR's class loader
     * @throws Exception if test GAR wasn't deployed
     */
    private ClassLoader getGarClassLoader() throws Exception {
        DeploymentResource task = getSpi().findResource("GridUriDeploymentTestWithNameTask7");

        assert task != null;

        return task.getClassLoader();
    }

    /**
     * @return List of URIs to use in this test.
     */
    @GridSpiTestConfig
    public List<String> getUriList() {
        return Collections.singletonList(GridTestProperties.getProperty("ant.urideployment.gar.uri").
            replace("EXTDATA", U.resolveIgnitePath("modules/extdata").getAbsolutePath()));
    }
}
