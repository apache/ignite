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

package org.gridgain.testsuites;

import junit.framework.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.lang.utils.*;
import org.gridgain.grid.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.future.nio.*;

/**
 * Gridgain language test suite.
 */
public class GridLangSelfTestSuite extends TestSuite {
    /**
     * @return Kernal test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain Lang Test Suite");

        suite.addTest(new TestSuite(GridFuncSelfTest.class));
        suite.addTest(new TestSuite(GridTupleSelfTest.class));
        suite.addTest(new TestSuite(GridByteArrayListSelfTest.class));
        suite.addTest(new TestSuite(GridLeanMapSelfTest.class));
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
        suite.addTest(new TestSuite(GridCacheConcurrentMapSelfTest.class));
        suite.addTest(new TestSuite(GridStripedLockSelfTest.class));

        suite.addTest(new TestSuite(GridFutureAdapterSelfTest.class));
        suite.addTest(new TestSuite(GridFinishedFutureSelfTest.class));
        suite.addTest(new TestSuite(GridCompoundFutureSelfTest.class));
        suite.addTest(new TestSuite(GridEmbeddedFutureSelfTest.class));
        suite.addTest(new TestSuite(GridNioFutureSelfTest.class));
        suite.addTest(new TestSuite(GridNioEmbeddedFutureSelfTest.class));

        // Consistent hash tests.
        suite.addTest(new TestSuite(GridConsistentHashSelfTest.class));

        return suite;
    }
}
