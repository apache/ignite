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

package org.apache.ignite.gridify;

import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.gridify.Gridify;
import org.apache.ignite.compute.gridify.GridifyArgument;
import org.apache.ignite.compute.gridify.GridifyTaskSplitAdapter;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.Collection;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tries to execute dummy gridified task. It should fail because grid is not started.
 * <p>
 * The main purpose of this test is to check that AOP is properly configured. It should
 * be included in all suites that require AOP.
 */
@RunWith(JUnit4.class)
public class BasicAopSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAop() throws Exception {
        try {
            gridify();

            fail();
        }
        catch (Exception e) {
            // No-op.
        }
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
            GridifyArgument arg) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Void reduce(List<ComputeJobResult> results) {
            return null;
        }
    }
}
