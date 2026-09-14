

package org.apache.ignite.console.testsuites;

import org.apache.ignite.console.messages.WebConsoleMessageSourceTest;
import org.apache.ignite.console.utils.UtilsTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Web Console "commons" test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    UtilsTest.class,
    WebConsoleMessageSourceTest.class
})
public class CommonTestSuite {
}
