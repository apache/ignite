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

import org.apache.ignite.internal.processors.cache.transactions.DepthFirstSearchTest;
import org.apache.ignite.internal.processors.cache.transactions.TxDeadlockDetectionMessageMarshallingTest;
import org.apache.ignite.internal.processors.cache.transactions.TxDeadlockDetectionNoHangsTest;
import org.apache.ignite.internal.processors.cache.transactions.TxDeadlockDetectionTest;
import org.apache.ignite.internal.processors.cache.transactions.TxDeadlockDetectionUnmasrhalErrorsTest;
import org.apache.ignite.internal.processors.cache.transactions.TxOptimisticDeadlockDetectionCrossCacheTest;
import org.apache.ignite.internal.processors.cache.transactions.TxOptimisticDeadlockDetectionTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPessimisticDeadlockDetectionCrossCacheTest;
import org.apache.ignite.internal.processors.cache.transactions.TxPessimisticDeadlockDetectionTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Deadlock detection related tests.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    DepthFirstSearchTest.class,
    TxOptimisticDeadlockDetectionTest.class,
    TxOptimisticDeadlockDetectionCrossCacheTest.class,
    TxPessimisticDeadlockDetectionTest.class,
    TxPessimisticDeadlockDetectionCrossCacheTest.class,
    //TxDeadlockCauseTest.class,
    TxDeadlockDetectionTest.class,
    TxDeadlockDetectionNoHangsTest.class,
    TxDeadlockDetectionUnmasrhalErrorsTest.class,
    TxDeadlockDetectionMessageMarshallingTest.class
})
public class TxDeadlockDetectionTestSuite {
}
