/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.concurrent.Callable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@GridCommonTest(group = "Utils")
@RunWith(JUnit4.class)
public class IgniteExceptionRegistrySelfTest extends GridCommonAbstractTest {
    /** */
    private IgniteExceptionRegistry registry = IgniteExceptionRegistry.get();

    /**
     * @throws Exception if failed.
     */
    @Test
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
    @Test
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
