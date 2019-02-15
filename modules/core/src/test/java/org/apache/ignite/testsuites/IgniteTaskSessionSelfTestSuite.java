/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
