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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.concurrent.Callable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 *
 */
@GridCommonTest(group = "Utils")
public class IgniteExceptionRegistrySelfTest extends GridCommonAbstractTest {
    /** */
    private IgniteExceptionRegistry registry = IgniteExceptionRegistry.get();

    /**
     * @throws Exception if failed.
     */
    public void testOnException() throws Exception {
        awaitPartitionMapExchange();

        int expCnt = 150;

        long errorCount = registry.errorCount();

        info(">>> Registry error count before test: " + errorCount);

        for (int i = 0; i < expCnt; i++)
            registry.onException("Test " + i, new Exception("Test " + i));

        Collection<IgniteExceptionRegistry.ExceptionInfo> errors = registry.getErrors(errorCount);

        int sz = errors.size();

        info(">>> Collected errors count after test: " + sz);

        if (expCnt != sz) {
            info(">>> Expected error count: " + expCnt + ", but actual: " + sz);

            for (IgniteExceptionRegistry.ExceptionInfo e : errors)
                if (!e.message().startsWith("Test ")) {
                    info("----------------------------");

                    info("!!! Found unexpected suppressed exception: msg=" + e.message() +
                        ", threadId=" + e.threadId() + ", threadName=" + e.threadName() +
                        ", err=" + e.error());

                    StringWriter sw = new StringWriter(1024);
                    e.error().printStackTrace(new PrintWriter(sw));
                    info(sw.toString());

                    info("----------------------------");
                }

            assert false;
        }

        int i = expCnt - 1;

        for (IgniteExceptionRegistry.ExceptionInfo e : errors) {
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

        int size = registry.getErrors(0).size();

        assert maxSize + 1 >= size && maxSize - 1 <= size;
    }
}