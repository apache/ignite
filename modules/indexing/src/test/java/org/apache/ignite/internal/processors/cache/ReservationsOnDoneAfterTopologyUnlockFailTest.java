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

package org.apache.ignite.internal.processors.cache;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.failure.TestFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 *
 */
public class ReservationsOnDoneAfterTopologyUnlockFailTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFailureHandlerTriggeredOnTopologyUnlockError() throws Exception {
        TestFailureHandler hnd = new TestFailureHandler(false);

        IgniteEx grid0 = startGrid(getConfiguration(testNodeName(0))
                .setFailureHandler(hnd));

        ExecutorService mockMgmtExecSvc = spy(grid0.context().getManagementExecutorService());
        doAnswer(invocationOnMock -> {
            Arrays.stream(Thread.currentThread().getStackTrace())
                    .map(StackTraceElement::getMethodName)
                    .filter("onDoneAfterTopologyUnlock"::equals)
                    .findAny()
                    .ifPresent(m -> {
                        throw new OutOfMemoryError();
                    });
            return invocationOnMock.callRealMethod();
        }).when(mockMgmtExecSvc).execute(any(Runnable.class));

        GridTestUtils.setFieldValue(grid(0).context(), "mgmtExecSvc", mockMgmtExecSvc);

        grid0.getOrCreateCache(new CacheConfiguration<>()
                .setName(DEFAULT_CACHE_NAME))
                .put(1, 1);

        startGrid(getConfiguration(testNodeName(1))
                .setFailureHandler(hnd));

        hnd.awaitFailure(3000);

        assertNotNull("Faulire handler hasn't been triggered.", hnd.failureContext());
        assertEquals("Failure type must be CRITICAL_ERROR", hnd.failureContext().type(), FailureType.CRITICAL_ERROR);
        assertTrue(hnd.failureContext().error() instanceof OutOfMemoryError);
    }
}
