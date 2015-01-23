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

package org.apache.ignite.gridify;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.compute.gridify.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Tries to execute dummy gridified task. It should fail because grid is not started.
 * <p>
 * The main purpose of this test is to check that AOP is properly configured. It should
 * be included in all suites that require AOP.
 */
public class BasicAopSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testAop() throws Exception {
        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    gridify();

                    return null;
                }
            },
            IgniteCheckedException.class,
            "Grid is not locally started: null"
        );
    }

    /**
     * Gridified method.
     */
    @Gridify(taskClass = TestTask.class)
    private void gridify() {
        // No-op
    }

    /**
     * Test task.
     */
    private static class TestTask extends GridifyTaskSplitAdapter<Void> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize,
            GridifyArgument arg) throws IgniteCheckedException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Void reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            return null;
        }
    }
}
