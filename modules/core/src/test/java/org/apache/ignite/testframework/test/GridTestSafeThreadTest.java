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

package org.apache.ignite.testframework.test;

import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestSafeThreadFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests {@link GridTestSafeThreadFactory}.
 */
public class GridTestSafeThreadTest extends GridCommonAbstractTest {
    /** */
    private static volatile IgniteInternalFuture<?> fut;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        assertTrue(fut.isDone());

        assertTrue(fut.error() instanceof IgniteInterruptedCheckedException);
    }

    /** Tests that hang threads will be stopped after test. */
    @Test
    @SuppressWarnings("InfiniteLoopStatement")
    public void testThreadHang() {
        fut = GridTestUtils.runAsync(() -> {
            while (true)
                U.sleep(1_000);
        });
    }
}
