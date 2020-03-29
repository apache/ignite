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
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Task session test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridSessionCancelSiblingsFromFutureSelfTest.class,
    GridSessionCancelSiblingsFromJobSelfTest.class,
    GridSessionCancelSiblingsFromTaskSelfTest.class,
    GridSessionSetFutureAttributeSelfTest.class,
    GridSessionSetFutureAttributeWaitListenerSelfTest.class,
    GridSessionSetJobAttributeWaitListenerSelfTest.class,
    GridSessionSetJobAttributeSelfTest.class,
    GridSessionSetJobAttribute2SelfTest.class,
    GridSessionJobWaitTaskAttributeSelfTest.class,
    GridSessionSetTaskAttributeSelfTest.class,
    GridSessionFutureWaitTaskAttributeSelfTest.class,
    GridSessionFutureWaitJobAttributeSelfTest.class,
    GridSessionTaskWaitJobAttributeSelfTest.class,
    GridSessionSetJobAttributeOrderSelfTest.class,
    GridSessionWaitAttributeSelfTest.class,
    GridSessionJobFailoverSelfTest.class,
    GridSessionLoadSelfTest.class,
    GridSessionCollisionSpiSelfTest.class,
    GridSessionCheckpointSelfTest.class
})
public class IgniteTaskSessionSelfTestSuite {
}
