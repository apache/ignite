/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.deployment.uri.scanners.file;

import org.gridgain.grid.spi.deployment.uri.*;
import org.gridgain.testframework.config.*;
import org.gridgain.testframework.junits.spi.*;
import java.util.*;

/**
 * Test file protocol scanner.
 */
@GridSpiTest(spi = GridUriDeploymentSpi.class, group = "Deployment SPI")
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
        checkTask("org.gridgain.grid.spi.deployment.uri.tasks.GridUriDeploymentTestTask0");
        checkTask("GridUriDeploymentTestWithNameTask0");
    }

    /**
     * Tests task from file 'deployfile.gar'.
     *
     * @throws Exception If failed.
     */
    public void testDeploymentFromFile() throws Exception {
        checkTask("org.gridgain.grid.spi.deployment.uri.tasks.GridUriDeploymentTestTask3");
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
        checkTask("org.gridgain.grid.spi.deployment.uri.tasks.GridUriDeploymentTestTask4");
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
        checkNoTask("org.gridgain.grid.spi.deployment.uri.tasks.GridUriDeploymentAbstractTestTask");
        checkNoTask("org.gridgain.grid.spi.deployment.uri.tasks.GridInnerTestTask");
        checkNoTask("org.gridgain.grid.spi.deployment.uri.tasks.GridUriDeploymentInterfaceTestTask");
        checkNoTask("org.gridgain.grid.spi.deployment.uri.tasks.GridUriDeploymentNonePublicTestTask");
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
        checkTask("org.gridgain.grid.spi.deployment.uri.tasks.GridUriDeploymentTestTask1");
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
        checkTask("org.gridgain.grid.spi.deployment.uri.tasks.GridUriDeploymentTestTask2");
    }

    /**
     * Tests task from files 'well-signed-deployfile.gar' and 'bad-signed-deployfile.gar'.
     * File 'bad-signed-deployfile.gar' contains non-signed modifications.
     *
     * Sign JAR with command:
     * $ jarsigner -keystore $GRIDGAIN_HOME/modules/tests/config/signeddeploy/keystore -storepass "abc123"
     *      -keypass "abc123" -signedjar signed-deployfile.gar deployfile.gar business
     *
     * Verify signed JAR-file:
     * $ jarsigner -verify -keystore $GRIDGAIN_HOME/modules/tests/config/signeddeploy/keystore -storepass "abc123"
     *      -keypass "abc123" signed-deployfile.gar
     *
     * @throws Exception If failed.
     */
    public void testSignedDeployment() throws Exception {
        checkTask("org.gridgain.grid.spi.deployment.uri.tasks.GridUriDeploymentTestTask5");
        checkTask("GridUriDeploymentTestWithNameTask5");

        assert getSpi().findResource("org.gridgain.grid.spi.deployment.uri.tasks.GridUriDeploymentTestTask6") == null :
            "Task from GAR with invalid signature should not be deployed.";
        assert getSpi().findResource("org.gridgain.grid.spi.deployment.uri.tasks.GridUriDeploymentTestWithNameTask6")
            == null : "Task from GAR with invalid signature should not be deployed.";
    }
}
