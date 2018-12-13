/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.testsuites;

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.session.GridSessionCancelSiblingsFromFutureSelfTest;
import org.apache.ignite.session.GridSessionCancelSiblingsFromJobSelfTest;
import org.apache.ignite.session.GridSessionCancelSiblingsFromTaskSelfTest;
import org.apache.ignite.session.GridSessionCheckpointSelfTest;
import org.apache.ignite.session.GridSessionCollisionSpiSelfTest;
import org.apache.ignite.session.GridSessionFutureWaitJobAttributeSelfTest;
import org.apache.ignite.session.GridSessionFutureWaitTaskAttributeSelfTest;
import org.apache.ignite.session.GridSessionJobFailoverSelfTest;
import org.apache.ignite.session.GridSessionJobWaitTaskAttributeSelfTest;
import org.apache.ignite.session.GridSessionLoadSelfTest;
import org.apache.ignite.session.GridSessionSetFutureAttributeSelfTest;
import org.apache.ignite.session.GridSessionSetFutureAttributeWaitListenerSelfTest;
import org.apache.ignite.session.GridSessionSetJobAttribute2SelfTest;
import org.apache.ignite.session.GridSessionSetJobAttributeOrderSelfTest;
import org.apache.ignite.session.GridSessionSetJobAttributeSelfTest;
import org.apache.ignite.session.GridSessionSetJobAttributeWaitListenerSelfTest;
import org.apache.ignite.session.GridSessionSetTaskAttributeSelfTest;
import org.apache.ignite.session.GridSessionTaskWaitJobAttributeSelfTest;
import org.apache.ignite.session.GridSessionWaitAttributeSelfTest;

/**
 * Task session test suite.
 */
public class IgniteTaskSessionSelfTestSuite extends TestSuite {
    /**
     * @return TaskSession test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite TaskSession Test Suite");

        suite.addTest(new JUnit4TestAdapter(GridSessionCancelSiblingsFromFutureSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSessionCancelSiblingsFromJobSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSessionCancelSiblingsFromTaskSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSessionSetFutureAttributeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSessionSetFutureAttributeWaitListenerSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSessionSetJobAttributeWaitListenerSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSessionSetJobAttributeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSessionSetJobAttribute2SelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSessionJobWaitTaskAttributeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSessionSetTaskAttributeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSessionFutureWaitTaskAttributeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSessionFutureWaitJobAttributeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSessionTaskWaitJobAttributeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSessionSetJobAttributeOrderSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSessionWaitAttributeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSessionJobFailoverSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSessionLoadSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSessionCollisionSpiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSessionCheckpointSelfTest.class));

        return suite;
    }
}
