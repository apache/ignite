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

        suite.addTest(new TestSuite(GridSessionCancelSiblingsFromFutureSelfTest.class));
        suite.addTest(new TestSuite(GridSessionCancelSiblingsFromJobSelfTest.class));
        suite.addTest(new TestSuite(GridSessionCancelSiblingsFromTaskSelfTest.class));
        suite.addTest(new TestSuite(GridSessionSetFutureAttributeSelfTest.class));
        suite.addTest(new TestSuite(GridSessionSetFutureAttributeWaitListenerSelfTest.class));
        suite.addTest(new TestSuite(GridSessionSetJobAttributeWaitListenerSelfTest.class));
        suite.addTest(new TestSuite(GridSessionSetJobAttributeSelfTest.class));
        suite.addTest(new TestSuite(GridSessionSetJobAttribute2SelfTest.class));
        suite.addTest(new TestSuite(GridSessionJobWaitTaskAttributeSelfTest.class));
        suite.addTest(new TestSuite(GridSessionSetTaskAttributeSelfTest.class));
        suite.addTest(new TestSuite(GridSessionFutureWaitTaskAttributeSelfTest.class));
        suite.addTest(new TestSuite(GridSessionFutureWaitJobAttributeSelfTest.class));
        suite.addTest(new TestSuite(GridSessionTaskWaitJobAttributeSelfTest.class));
        suite.addTest(new TestSuite(GridSessionSetJobAttributeOrderSelfTest.class));
        suite.addTest(new TestSuite(GridSessionWaitAttributeSelfTest.class));
        suite.addTest(new TestSuite(GridSessionJobFailoverSelfTest.class));
        suite.addTest(new TestSuite(GridSessionLoadSelfTest.class));
        suite.addTest(new TestSuite(GridSessionCollisionSpiSelfTest.class));
        suite.addTest(new TestSuite(GridSessionCheckpointSelfTest.class));

        return suite;
    }
}