package org.apache.ignite.mesos;

import junit.framework.TestCase;
import org.apache.mesos.Protos;
import org.hamcrest.core.Is;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.omg.CORBA.Environment;

import static org.junit.Assert.assertThat;

/**
 * Created by sbt-opolskiy-va on 28.03.2017.
 */
public class IgniteFrameworkInfoTest extends TestCase {

    /**
     * Framework name.
     */
    private final String IGNITE_FRAMEWORK_NAME = "Ignite";

    /**
     * Mesos user name in system environment.
     */
    private final String MESOS_USER_NAME = "MESOS_USER";

    private final String testName = "mesosusername";

    /**
     * Mesos user name in system environment.
     */
    private final String MESOS_ROLE = "MESOS_ROLE";

    private final String testRole = "mesosrole";

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Override
    public void setUp() throws Exception {
        environmentVariables.set(MESOS_USER_NAME, testName);
        environmentVariables.set(MESOS_ROLE, testRole);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFrameworkInfo() throws Exception {

        String name = System.getenv(MESOS_USER_NAME);
        String role = System.getenv(MESOS_ROLE);

        Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo.newBuilder()
            .setName(IGNITE_FRAMEWORK_NAME)
            .setUser(name != null ? name : "")
            .setRole(role != null ? role : "*");


        assertThat(testName, Is.is(name));
        assertThat(testRole, Is.is(role));

        assertThat(name, Is.is(frameworkBuilder.getUser()));
        assertThat(role, Is.is(frameworkBuilder.getRole()));
        assertThat(IGNITE_FRAMEWORK_NAME, Is.is(frameworkBuilder.getName()));
    }
}