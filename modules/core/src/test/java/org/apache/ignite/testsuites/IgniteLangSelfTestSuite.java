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

import java.util.ArrayList;
import java.util.List;
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
import org.junit.runners.model.InitializationError;

/**
 * Ignite language test suite.
 */
@RunWith(IgniteLangSelfTestSuite.DynamicSuite.class)
public class IgniteLangSelfTestSuite {
    /**
     * @return Kernal test suite.
     */
    public static List<Class<?>> suite() {
        List<Class<?>> suite = new ArrayList<>();

        suite.add(GridTupleSelfTest.class);
        suite.add(GridBoundedPriorityQueueSelfTest.class);
        suite.add(GridByteArrayListSelfTest.class);
        suite.add(GridLeanMapSelfTest.class);
        suite.add(GridLeanIdentitySetSelfTest.class);
        suite.add(GridListSetSelfTest.class);
        suite.add(GridSetWrapperSelfTest.class);
        suite.add(GridConcurrentWeakHashSetSelfTest.class);
        suite.add(GridMetadataAwareAdapterSelfTest.class);
        suite.add(GridSetWrapperSelfTest.class);
        suite.add(IgniteUuidSelfTest.class);
        suite.add(GridXSelfTest.class);
        suite.add(GridBoundedConcurrentOrderedMapSelfTest.class);
        suite.add(GridBoundedConcurrentLinkedHashMapSelfTest.class);
        suite.add(GridConcurrentLinkedDequeSelfTest.class);
        suite.add(GridCircularBufferSelfTest.class);
        suite.add(GridConcurrentLinkedHashMapSelfTest.class);
        suite.add(GridConcurrentLinkedHashMapMultiThreadedSelfTest.class);
        suite.add(GridStripedLockSelfTest.class);

        suite.add(GridFutureAdapterSelfTest.class);
        suite.add(GridCompoundFutureSelfTest.class);
        suite.add(GridEmbeddedFutureSelfTest.class);
        suite.add(GridNioFutureSelfTest.class);
        suite.add(GridNioEmbeddedFutureSelfTest.class);

        suite.add(IgniteFutureImplTest.class);
        suite.add(IgniteCacheFutureImplTest.class);

        suite.add(IgniteOffheapReadWriteLockSelfTest.class);

        // Consistent hash tests.
        suite.add(GridConsistentHashSelfTest.class);

        return suite;
    }

    /** */
    public static class DynamicSuite extends Suite {
        /** */
        public DynamicSuite(Class<?> cls) throws InitializationError {
            super(cls, suite().toArray(new Class<?>[] {null}));
        }
    }
}
