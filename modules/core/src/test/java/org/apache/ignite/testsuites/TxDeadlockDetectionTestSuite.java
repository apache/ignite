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

package org.apache.ignite.testsuites;

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.transactions.DepthFirstSearchTest;
import org.apache.ignite.internal.processors.cache.transactions.TxDeadlockCauseTest;
import org.apache.ignite.internal.processors.cache.transactions.TxDeadlockDetectionMessageMarshallingTest;
import org.apache.ignite.internal.processors.cache.transactions.TxDeadlockDetectionNoHangsTest;
import org.apache.ignite.internal.processors.cache.transactions.TxDeadlockDetectionTest;
import org.apache.ignite.internal.processors.cache.transactions.TxDeadlockDetectionUnmasrhalErrorsTest;
import org.apache.ignite.internal.processors.cache.transactions.TxOptimisticDeadlockDetectionCrossCacheTest;
import org.apache.ignite.internal.processors.cache.transactions.TxOptimisticDeadlockDetectionTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPessimisticDeadlockDetectionCrossCacheTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPessimisticDeadlockDetectionTest;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Deadlock detection related tests.
 */
@RunWith(AllTests.class)
public class TxDeadlockDetectionTestSuite {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Deadlock Detection Test Suite");

        suite.addTest(new JUnit4TestAdapter(DepthFirstSearchTest.class));
        suite.addTest(new JUnit4TestAdapter(TxOptimisticDeadlockDetectionTest.class));
        suite.addTest(new JUnit4TestAdapter(TxOptimisticDeadlockDetectionCrossCacheTest.class));
        suite.addTest(new JUnit4TestAdapter(TxPessimisticDeadlockDetectionTest.class));
        suite.addTest(new JUnit4TestAdapter(TxPessimisticDeadlockDetectionCrossCacheTest.class));
        //suite.addTest(new JUnit4TestAdapter(TxDeadlockCauseTest.class));
        suite.addTest(new JUnit4TestAdapter(TxDeadlockDetectionTest.class));
        suite.addTest(new JUnit4TestAdapter(TxDeadlockDetectionNoHangsTest.class));
        suite.addTest(new JUnit4TestAdapter(TxDeadlockDetectionUnmasrhalErrorsTest.class));
        suite.addTest(new JUnit4TestAdapter(TxDeadlockDetectionMessageMarshallingTest.class));

        return suite;
    }
}
