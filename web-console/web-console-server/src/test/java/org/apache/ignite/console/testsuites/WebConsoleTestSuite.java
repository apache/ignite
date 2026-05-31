

package org.apache.ignite.console.testsuites;


import org.apache.ignite.console.listener.NotificationEventListenerTest;
import org.apache.ignite.console.repositories.AccountsRepositoryTest;
import org.apache.ignite.console.db.TableSelfTest;
import org.apache.ignite.console.services.AccountServiceTest;
import org.apache.ignite.console.services.ActivitiesServiceTest;
import org.apache.ignite.console.services.AdminServiceTest;
import org.apache.ignite.console.services.NotificationServiceTest;
import org.apache.ignite.console.web.security.PasswordEncoderTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Ignite Web Console test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    AccountServiceTest.class,
    PasswordEncoderTest.class,
    TableSelfTest.class,
    ActivitiesServiceTest.class,
    AdminServiceTest.class,
    NotificationEventListenerTest.class,
    AccountsRepositoryTest.class,
    NotificationServiceTest.class
   
})
public class WebConsoleTestSuite {
}
