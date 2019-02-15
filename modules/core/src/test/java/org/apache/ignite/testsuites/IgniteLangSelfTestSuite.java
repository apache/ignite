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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.internal.util.future.GridCompoundFutureSelfTest;
import org.apache.ignite.internal.util.future.GridEmbeddedFutureSelfTest;
import org.apache.ignite.internal.util.future.GridFutureAdapterSelfTest;
import org.apache.ignite.internal.util.future.IgniteCacheFutureImplTest;
import org.apache.ignite.internal.util.future.IgniteFutureImplTest;
import org.apache.ignite.internal.util.future.nio.GridNioEmbeddedFutureSelfTest;
import org.apache.ignite.internal.util.future.nio.GridNioFutureSelfTest;
import org.apache.ignite.lang.GridByteArrayListSelfTest;
import org.apache.ignite.lang.GridMetadataAwareAdapterSelfTest;
import org.apache.ignite.lang.GridSetWrapperSelfTest;
import org.apache.ignite.lang.GridTupleSelfTest;
import org.apache.ignite.lang.GridXSelfTest;
import org.apache.ignite.lang.IgniteUuidSelfTest;
import org.apache.ignite.lang.utils.GridBoundedConcurrentLinkedHashMapSelfTest;
import org.apache.ignite.lang.utils.GridBoundedConcurrentOrderedMapSelfTest;
import org.apache.ignite.lang.utils.GridBoundedPriorityQueueSelfTest;
import org.apache.ignite.lang.utils.GridCircularBufferSelfTest;
import org.apache.ignite.lang.utils.GridConcurrentLinkedHashMapSelfTest;
import org.apache.ignite.lang.utils.GridConcurrentWeakHashSetSelfTest;
import org.apache.ignite.lang.utils.GridConsistentHashSelfTest;
import org.apache.ignite.lang.utils.GridLeanIdentitySetSelfTest;
import org.apache.ignite.lang.utils.GridLeanMapSelfTest;
import org.apache.ignite.lang.utils.GridListSetSelfTest;
import org.apache.ignite.lang.utils.GridStripedLockSelfTest;
import org.apache.ignite.lang.utils.IgniteOffheapReadWriteLockSelfTest;
import org.apache.ignite.util.GridConcurrentLinkedDequeSelfTest;
import org.apache.ignite.util.GridConcurrentLinkedHashMapMultiThreadedSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Ignite language test suite.
 */
@RunWith(AllTests.class)
public class IgniteLangSelfTestSuite {
    /**
     * @return Kernal test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Lang Test Suite");

        suite.addTest(new JUnit4TestAdapter(GridTupleSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridBoundedPriorityQueueSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridByteArrayListSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridLeanMapSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridLeanIdentitySetSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridListSetSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSetWrapperSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridConcurrentWeakHashSetSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridMetadataAwareAdapterSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridSetWrapperSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteUuidSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridXSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridBoundedConcurrentOrderedMapSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridBoundedConcurrentLinkedHashMapSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridConcurrentLinkedDequeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCircularBufferSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridConcurrentLinkedHashMapSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridConcurrentLinkedHashMapMultiThreadedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridStripedLockSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridFutureAdapterSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCompoundFutureSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridEmbeddedFutureSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridNioFutureSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridNioEmbeddedFutureSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteFutureImplTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheFutureImplTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteOffheapReadWriteLockSelfTest.class));

        // Consistent hash tests.
        suite.addTest(new JUnit4TestAdapter(GridConsistentHashSelfTest.class));

        return suite;
    }
}
