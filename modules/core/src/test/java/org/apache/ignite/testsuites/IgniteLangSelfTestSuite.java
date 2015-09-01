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
import org.apache.ignite.internal.util.future.GridCompoundFutureSelfTest;
import org.apache.ignite.internal.util.future.GridEmbeddedFutureSelfTest;
import org.apache.ignite.internal.util.future.GridFutureAdapterSelfTest;
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
import org.apache.ignite.lang.utils.GridCircularBufferSelfTest;
import org.apache.ignite.lang.utils.GridConcurrentLinkedHashMapSelfTest;
import org.apache.ignite.lang.utils.GridConcurrentWeakHashSetSelfTest;
import org.apache.ignite.lang.utils.GridConsistentHashSelfTest;
import org.apache.ignite.lang.utils.GridLeanIdentitySetSelfTest;
import org.apache.ignite.lang.utils.GridLeanMapSelfTest;
import org.apache.ignite.lang.utils.GridListSetSelfTest;
import org.apache.ignite.lang.utils.GridStripedLockSelfTest;
import org.apache.ignite.util.GridConcurrentLinkedDequeSelfTest;
import org.apache.ignite.util.GridConcurrentLinkedHashMapMultiThreadedSelfTest;

/**
 * Ignite language test suite.
 */
public class IgniteLangSelfTestSuite extends TestSuite {
    /**
     * @return Kernal test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite Lang Test Suite");

        suite.addTest(new TestSuite(GridTupleSelfTest.class));
        suite.addTest(new TestSuite(GridByteArrayListSelfTest.class));
        suite.addTest(new TestSuite(GridLeanMapSelfTest.class));
        suite.addTest(new TestSuite(GridLeanIdentitySetSelfTest.class));
        suite.addTest(new TestSuite(GridListSetSelfTest.class));
        suite.addTest(new TestSuite(GridSetWrapperSelfTest.class));
        suite.addTest(new TestSuite(GridConcurrentWeakHashSetSelfTest.class));
        suite.addTest(new TestSuite(GridMetadataAwareAdapterSelfTest.class));
        suite.addTest(new TestSuite(GridSetWrapperSelfTest.class));
        suite.addTest(new TestSuite(IgniteUuidSelfTest.class));
        suite.addTest(new TestSuite(GridXSelfTest.class));
        suite.addTest(new TestSuite(GridBoundedConcurrentOrderedMapSelfTest.class));
        suite.addTest(new TestSuite(GridBoundedConcurrentLinkedHashMapSelfTest.class));
        suite.addTest(new TestSuite(GridConcurrentLinkedDequeSelfTest.class));
        suite.addTest(new TestSuite(GridCircularBufferSelfTest.class));
        suite.addTest(new TestSuite(GridConcurrentLinkedHashMapSelfTest.class));
        suite.addTest(new TestSuite(GridConcurrentLinkedHashMapMultiThreadedSelfTest.class));
        suite.addTest(new TestSuite(GridStripedLockSelfTest.class));

        suite.addTest(new TestSuite(GridFutureAdapterSelfTest.class));
        suite.addTest(new TestSuite(GridCompoundFutureSelfTest.class));
        suite.addTest(new TestSuite(GridEmbeddedFutureSelfTest.class));
        suite.addTest(new TestSuite(GridNioFutureSelfTest.class));
        suite.addTest(new TestSuite(GridNioEmbeddedFutureSelfTest.class));
        suite.addTest(new TestSuite(IgniteFutureImplTest.class));

        // Consistent hash tests.
        suite.addTest(new TestSuite(GridConsistentHashSelfTest.class));

        return suite;
    }
}