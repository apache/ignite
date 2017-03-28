package org.apache.ignite.mesos;

import junit.framework.TestCase;
import org.apache.mesos.Protos;
import org.hamcrest.core.Is;

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

    /**
     * Mesos user name in system environment.
     */
    private final String MESOS_ROLE = "MESOS_ROLE";

    private String userName;

    private String mesosRole;

    private final int frameworkFailoverTimeout = 0;


    @Override
    public void setUp() throws Exception {

        userName = (System.getenv(MESOS_USER_NAME) != null
                ? System.getenv(MESOS_USER_NAME) : "");

        mesosRole = System.getenv(MESOS_ROLE) != null
                ? System.getenv(MESOS_ROLE) : "*";
    }

    /**
     * @throws Exception If failed.
     */
    public void testFrameworkInfo() throws Exception {

        Protos.FrameworkInfo.Builder frameworkBuilder = Protos.FrameworkInfo.newBuilder()
                .setName(IGNITE_FRAMEWORK_NAME)
                .setUser(userName != null ? userName : "")
                .setRole(mesosRole != null ? mesosRole : "*")
                .setFailoverTimeout(frameworkFailoverTimeout);

        assertThat(userName, Is.is(frameworkBuilder.getUser()));
        assertThat(mesosRole, Is.is(frameworkBuilder.getRole()));
        assertThat(IGNITE_FRAMEWORK_NAME, Is.is(frameworkBuilder.getName()));
        assertThat((double) frameworkFailoverTimeout, Is.is(frameworkBuilder.getFailoverTimeout()));

    }

}
