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

import java.util.Collections;
import java.util.List;
import org.apache.ignite.spi.deployment.uri.GridUriDeploymentAbstractSelfTest;
import org.apache.ignite.spi.deployment.uri.UriDeploymentSpi;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTestConfig;

/**
 * Test file protocol scanner.
 */
@GridSpiTest(spi = UriDeploymentSpi.class, group = "Deployment SPI")
public class GridFileDeploymentSelfTest extends GridUriDeploymentAbstractSelfTest {
    /**
     * @return List of URI to use as deployment source.
     */
    @GridSpiTestConfig
    public List<String> getUriList() {
        return Collections.singletonList(GridTestProperties.getProperty("deploy.uri.file"));
    }

    /**
     * Tests task from folder 'deploydir.gar'.
     *
     * @throws Exception If failed.
     */
    public void testDeploymentFromFolder() throws Exception {
        checkTask("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask0");
        checkTask("GridUriDeploymentTestWithNameTask0");
    }

    /**
     * Tests task from file 'deployfile.gar'.
     *
     * @throws Exception If failed.
     */
    public void testDeploymentFromFile() throws Exception {
        checkTask("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask3");
        checkTask("GridUriDeploymentTestWithNameTask3");
    }

    /**
     * Tests task from file 'deployfile_nodescr.gar'.
     *
     * Looks for task {@code GridUriDeploymentTestTask4} without descriptor file from GAR-file.
     * That task loads resource {@code spring.xml}.
     *
     * To check {@code GridDeploymentUriClassLoader} class loader you need to delete all classes
     * and resources from Junit classpath. Note that class loader searches classes in a GAR file and
     * not in the parent class loader for junits.
     *
     * @throws Exception If failed.
     */
    public void testNoDescriptorDeployment() throws Exception {
        checkTask("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask4");
        checkTask("GridUriDeploymentTestWithNameTask4");
    }

    /**
     * Tests task from file 'deployfile_bad.gar'.
     *
     * Looks for tasks {@code GridUriDeploymentAbstractTestTask}
     * {@code GridInnerTestTask}
     * {@code GridUriDeploymentInterfaceTestTask}
     * {@code GridUriDeploymentNonePublicTestTask} from GAR-file. Tasks should not be deployed.
     *
     * @throws Exception If failed.
     */
    public void testBadDeployment() throws Exception {
        checkNoTask("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentAbstractTestTask");
        checkNoTask("org.apache.ignite.spi.deployment.uri.tasks.GridInnerTestTask");
        checkNoTask("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentInterfaceTestTask");
        checkNoTask("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentNonePublicTestTask");
    }

    /**
     * Tests task from file 'deploy_depend.gar'.
     *
     * Looks for task {@code GridUriDeploymentTestTask1} with descriptor file from GAR-file.
     * That task loads resource {@code spring1.xml} and imports external class from /lib/*.jar
     * External class loads resource {@code test1.properties}from the same JAR it is loaded from.
     *
     * To check {@code GridDeploymentUriClassLoader} class loader need to delete all classes
     * and resources from Junit classpath. Note that class loader searches for classes in a GAR
     * file and not in the parent class loader for junits.
     *
     * @throws Exception If failed.
     */
    public void testDependenceDeployment() throws Exception {
        checkTask("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask1");
    }

    /**
     * Tests task from file 'deploy_nodescr_depend.gar'.
     *
     * Looks for task {@code GridUriDeploymentTestTask2} without descriptor file from GAR-file.
     * That task loads resource {@code spring2.xml} and imports external class from /lib/*.jar
     * External class loads resource {@code test2.properties}from the same JAR it is loaded from.
     *
     * To check {@code GridDeploymentUriClassLoader} class loader need to delete all classes
     * and resources from Junit classpath. Note that class loader searches for classes in a GAR
     * file and not in the parent class loader for junits.
     *
     * @throws Exception If failed.
     */
    public void testNoDescriptorDependenceDeployment() throws Exception {
        checkTask("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask2");
    }

    /**
     * Tests task from files 'well-signed-deployfile.gar' and 'bad-signed-deployfile.gar'.
     * File 'bad-signed-deployfile.gar' contains non-signed modifications.
     *
     * Sign JAR with command:
     * $ jarsigner -keystore $IGNITE_HOME/modules/tests/config/signeddeploy/keystore -storepass "abc123"
     *      -keypass "abc123" -signedjar signed-deployfile.gar deployfile.gar business
     *
     * Verify signed JAR-file:
     * $ jarsigner -verify -keystore $IGNITE_HOME/modules/tests/config/signeddeploy/keystore -storepass "abc123"
     *      -keypass "abc123" signed-deployfile.gar
     *
     * @throws Exception If failed.
     */
    public void testSignedDeployment() throws Exception {
        checkTask("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask5");
        checkTask("GridUriDeploymentTestWithNameTask5");

        assert getSpi().findResource("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask6") == null :
            "Task from GAR with invalid signature should not be deployed.";
        assert getSpi().findResource("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestWithNameTask6")
            == null : "Task from GAR with invalid signature should not be deployed.";
    }
}