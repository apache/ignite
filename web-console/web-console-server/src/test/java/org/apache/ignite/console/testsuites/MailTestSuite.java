

package org.apache.ignite.console.testsuites;

import org.apache.ignite.console.services.MailServiceTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Mail service test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    MailServiceTest.class,
})
public class MailTestSuite {
}
