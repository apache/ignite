/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.h2.test;

import java.util.Timer;
import java.util.TimerTask;
import junit.framework.Protectable;
import junit.framework.Test;
import junit.framework.TestResult;
import org.h2.util.ThreadDeadlockDetector;
import org.junit.runner.Describable;
import org.junit.runner.Description;

/**
 * H2 test runner Contains H2 test with config and provides correct description for JUnit engine.
 */
class H2TestCase implements Test, Describable {
    /** Config holder. */
    private TestAll conf;

    /** Test to run. */
    private TestBase test;

    /**
     * Constructor.
     *
     * @param conf Context for test.
     * @param test H2 test implementation.
     */
    H2TestCase(TestAll conf, TestBase test) {
        this.conf = conf;
        this.test = test;
    }

    /** {@inheritDoc} */
    @Override public int countTestCases() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public void run(TestResult result) {
        result.startTest(this);
        // event queue watchdog for tests that get stuck when running in Jenkins
        final Timer watchdog = new Timer();
        // 5 minutes
        watchdog.schedule(new TimerTask() {
            @Override
            public void run() {
                ThreadDeadlockDetector.dumpAllThreadsAndLocks("test watchdog timed out");
            }
        }, 5 * 60 * 1000);
        try {
            result.startTest(this);
            Protectable p = new Protectable() {
                public void protect() throws Throwable {
                    test.runTest(conf);
                }
            };
            result.runProtected(this, p);
        }
        finally {
            watchdog.cancel();
            result.endTest(this);
        }
    }

    /** {@inheritDoc} */
    @Override public Description getDescription() {
        return Description.createTestDescription(test.getClass(), "test[" + conf.toString() + "]");
    }
}
