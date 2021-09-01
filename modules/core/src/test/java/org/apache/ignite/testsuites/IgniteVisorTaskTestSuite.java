package org.apache.ignite.testsuites;

import org.apache.ignite.internal.visor.tx.VisorTxTaskExecutionTest;
import org.apache.ignite.internal.visor.tx.VisorTxTaskTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for Visor task execution.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    VisorTxTaskTest.class,
    VisorTxTaskExecutionTest.class
})
public class IgniteVisorTaskTestSuite {
}
