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
import org.junit.Test;

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
    @Test
    public void testDeploymentFromFolder() throws Exception {
        checkTask("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask0");
        checkTask("GridUriDeploymentTestWithNameTask0");
    }

    /**
     * Tests task from file 'deployfile.jar'.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeploymentFromJar() throws Exception {
        checkTask("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask8");
        checkTask("GridUriDeploymentTestWithNameTask8");
    }

    /**
     * Tests task from file 'deployfile.gar'.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDeploymentFromGar() throws Exception {
        checkTask("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask3");
        checkTask("GridUriDeploymentTestWithNameTask3");
    }

    /**
     * Tests task from file 'deployfile-nodescr.gar'.
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
    @Test
    public void testNoDescriptorDeployment() throws Exception {
        checkTask("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask4");
        checkTask("GridUriDeploymentTestWithNameTask4");
    }

    /**
     * Tests task from file 'deployfile-bad.gar'.
     *
     * Looks for tasks {@code GridUriDeploymentAbstractTestTask}
     * {@code GridInnerTestTask}
     * {@code GridUriDeploymentInterfaceTestTask}
     * {@code GridUriDeploymentNonePublicTestTask} from GAR-file. Tasks should not be deployed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBadDeployment() throws Exception {
        checkNoTask("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentAbstractTestTask");

        checkNoTask("org.apache.ignite.spi.deployment.uri.tasks.GridInnerTestTask");
        checkNoTask("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentInnerTestTask$GridInnerTestTask");
        checkNoTask("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentInnerTestTask.GridInnerTestTask");

        checkNoTask("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentInterfaceTestTask");
        checkNoTask("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentNonePublicTestTask");
    }

    /**
     * Tests task from file 'deployfile.jar'.
     *
     * Looks for task {@code GridUriDeploymentTestTask9}.
     * That task loads resource {@code spring9.xml} and imports external class from the same JAR
     * External class loads resource {@code test9.properties} from the same JAR it is loaded from.
     *
     * To check {@code GridDeploymentUriClassLoader} class loader need to delete all classes
     * and resources from Junit classpath. Note that class loader searches for classes in a JAR
     * file and not in the parent class loader for junits.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDependenceJarDeployment() throws Exception {
        checkTask("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask9");
        getSpi().findResource("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask9")
                .getResourceClass().newInstance();
    }

    /**
     * Tests task from file 'deployfile-depend.gar'.
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
    @Test
    public void testDependenceGarDeployment() throws Exception {
        checkTask("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask1");
        getSpi().findResource("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask1")
            .getResourceClass().newInstance();
    }

    /**
     * Tests task from file 'deploydir-nodescr-depend.gar'.
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
    @Test
    public void testNoDescriptorDependenceDeployment() throws Exception {
        checkTask("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask2");
        getSpi().findResource("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask2")
            .getResourceClass().newInstance();
    }

    /**
     * Tests task from files 'well-signed-deployfile.gar', 'bad-signed-deployfile.gar',
     * 'well-signed-deployfile.jar' and 'bad-signed-deployfile.jar'.
     * Files 'bad-signed-deployfile.gar', 'bad-signed-deployfile.jar' contain non-signed modifications.
     *
     * Sign JAR with command:
     * $ jarsigner -keystore $IGNITE_HOME/modules/tests/config/signeddeploy/keystore -storepass "abc123"
     *      -keypass "abc123" -signedjar signed-deployfile.jar deployfile.jar business
     *
     * Verify signed JAR-file:
     * $ jarsigner -verify -keystore $IGNITE_HOME/modules/tests/config/signeddeploy/keystore -storepass "abc123"
     *      -keypass "abc123" signed-deployfile.jar
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSignedDeployment() throws Exception {
        checkTask("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask5");
        checkTask("GridUriDeploymentTestWithNameTask5");
        checkTask("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask10");
        checkTask("GridUriDeploymentTestWithNameTask10");

        assert getSpi().findResource("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask6") == null :
            "Task from GAR with invalid signature should not be deployed.";
        assert getSpi().findResource("tasks.GridUriDeploymentTestWithNameTask6")
            == null : "Task from GAR with invalid signature should not be deployed.";
        assert getSpi().findResource("org.apache.ignite.spi.deployment.uri.tasks.GridUriDeploymentTestTask11") == null :
            "Task from JAR with invalid signature should not be deployed.";
        assert getSpi().findResource("GridUriDeploymentTestWithNameTask11")
            == null : "Task from JAR with invalid signature should not be deployed.";
    }
}
