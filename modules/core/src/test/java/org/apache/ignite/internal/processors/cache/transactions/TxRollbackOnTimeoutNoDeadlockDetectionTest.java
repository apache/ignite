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

package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.transactions.TransactionTimeoutException;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_TX_DEADLOCK_DETECTION_MAX_ITERS;

/**
 * Tests an ability to eagerly rollback timed out transactions.
 */
public class TxRollbackOnTimeoutNoDeadlockDetectionTest extends TxRollbackOnTimeoutTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(IGNITE_TX_DEADLOCK_DETECTION_MAX_ITERS, "0");

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        System.clearProperty(IGNITE_TX_DEADLOCK_DETECTION_MAX_ITERS);
    }

    /** */
    @Override protected void validateDeadlockException(Exception e) {
        assertEquals("TimeoutException is expected",
            TransactionTimeoutException.class, e.getCause().getClass());
    }
}
