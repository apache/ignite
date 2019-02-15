/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.spi.deployment.uri;

import java.net.URL;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Grid URI deployment class loader self test.
 */
@RunWith(JUnit4.class)
public class GridUriDeploymentClassLoaderMultiThreadedSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMultiThreadedClassLoading() throws Exception {
        for (int i = 0; i < 50; i++)
            doTest();
    }

    /**
     * @throws Exception If failed.
     */
    private void doTest() throws Exception {
        final GridUriDeploymentClassLoader ldr = new GridUriDeploymentClassLoader(
            new URL[] { U.resolveIgniteUrl(GridTestProperties.getProperty("ant.urideployment.gar.file")) },
                getClass().getClassLoader());

        multithreaded(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    ldr.loadClass("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask0");

                    return null;
                }
            },
            500
        );

        final GridUriDeploymentClassLoader ldr0 = new GridUriDeploymentClassLoader(
            new URL[] { U.resolveIgniteUrl(GridTestProperties.getProperty("ant.urideployment.gar.file")) },
            getClass().getClassLoader());

        multithreaded(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    ldr0.loadClassGarOnly("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask0");

                    return null;
                }
            },
            500
        );
    }
}
