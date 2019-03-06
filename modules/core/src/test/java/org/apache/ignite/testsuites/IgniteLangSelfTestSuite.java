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
import org.junit.runners.Suite;

/**
 * Ignite language test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridTupleSelfTest.class,
    GridBoundedPriorityQueueSelfTest.class,
    GridByteArrayListSelfTest.class,
    GridLeanMapSelfTest.class,
    GridLeanIdentitySetSelfTest.class,
    GridListSetSelfTest.class,
    GridSetWrapperSelfTest.class,
    GridConcurrentWeakHashSetSelfTest.class,
    GridMetadataAwareAdapterSelfTest.class,
    GridSetWrapperSelfTest.class,
    IgniteUuidSelfTest.class,
    GridXSelfTest.class,
    GridBoundedConcurrentOrderedMapSelfTest.class,
    GridBoundedConcurrentLinkedHashMapSelfTest.class,
    GridConcurrentLinkedDequeSelfTest.class,
    GridCircularBufferSelfTest.class,
    GridConcurrentLinkedHashMapSelfTest.class,
    GridConcurrentLinkedHashMapMultiThreadedSelfTest.class,
    GridStripedLockSelfTest.class,

    GridFutureAdapterSelfTest.class,
    GridCompoundFutureSelfTest.class,
    GridEmbeddedFutureSelfTest.class,
    GridNioFutureSelfTest.class,
    GridNioEmbeddedFutureSelfTest.class,

    IgniteFutureImplTest.class,
    IgniteCacheFutureImplTest.class,

    IgniteOffheapReadWriteLockSelfTest.class,

    // Consistent hash tests.
    GridConsistentHashSelfTest.class,
})
public class IgniteLangSelfTestSuite {
}
