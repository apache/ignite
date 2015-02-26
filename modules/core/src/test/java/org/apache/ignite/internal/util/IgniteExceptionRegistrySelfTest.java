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

package org.apache.ignite.internal.util;

import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

/**
 *
 */
@GridCommonTest(group = "Utils")
public class IgniteExceptionRegistrySelfTest extends GridCommonAbstractTest {
    /** */
    private IgniteExceptionRegistry registry;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        registry = new IgniteExceptionRegistry(new GridStringLogger());
    }

    /**
     * @throws Exception if failed.
     */
    public void testOnException() throws Exception {
        int expCnt = 150;

        for (int i = 0; i < expCnt; i++)
            registry.onException("Test " + i, new Exception("Test " + i));

        Collection<IgniteExceptionRegistry.ExceptionInfo> exceptions = registry.getErrors();

        assertEquals(expCnt, registry.getErrors().size());
        assertEquals(expCnt, registry.getErrors().size());
        assertEquals(expCnt, registry.getErrors().size());

        int i = expCnt - 1;

        for (IgniteExceptionRegistry.ExceptionInfo e : exceptions) {
            assertNotNull(e);
            assertEquals(e.message(), "Test " + i);
            assertEquals(e.threadId(), Thread.currentThread().getId());
            assertEquals(e.threadName(), Thread.currentThread().getName());

            --i;
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testMultiThreadedMaxSize() throws Exception {
        final int maxSize = 10;

        registry.setMaxSize(maxSize);

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int i = 0; i < maxSize; i++)
                    registry.onException("Test " + i, new Exception("test"));

                return null;
            }
        }, 10, "TestSetMaxSize");

        int size = registry.getErrors().size();

        assert maxSize + 1 >= size && maxSize - 1 <= size;
    }
}