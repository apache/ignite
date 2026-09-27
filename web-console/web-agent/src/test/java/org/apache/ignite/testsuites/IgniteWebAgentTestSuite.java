

package org.apache.ignite.testsuites;

import org.apache.ignite.console.agent.AgentLauncherTest;
import org.apache.ignite.console.agent.AgentUtilsTest;
import org.apache.ignite.websocket.SerializationTests;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Web Agent test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    AgentLauncherTest.class,
    AgentUtilsTest.class,  
    SerializationTests.class
})
public class IgniteWebAgentTestSuite {
    // No-op.
}
