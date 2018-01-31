package org.apache.ignite.testsuites;

import org.apache.ignite.internal.processors.cache.persistence.pagemem.IgniteThrottlingUnitTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 *
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    IgniteThrottlingUnitTest.class
})
public class IgnitePdsUnitTestSuite {
}
