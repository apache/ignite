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

package org.apache.ignite.internal.managers.checkpoint;

import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@GridCommonTest(group = "Checkpoint Manager")
@RunWith(JUnit4.class)
public class GridCheckpointManagerSelfTest extends GridCheckpointManagerAbstractSelfTest {
    /**
     * @throws Exception Thrown if any exception occurs.
     */
    @Test
    public void testCacheBased() throws Exception {
        doTest("cache");
    }

    /**
     * @throws Exception Thrown if any exception occurs.
     */
    @Test
    public void testSharedFsBased() throws Exception {
        doTest("sharedfs");
    }

    /**
     * @throws Exception Thrown if any exception occurs.
     */
    @Test
    public void testDatabaseBased() throws Exception {
        doTest("jdbc");
    }

    /**
     * @throws Exception Thrown if any exception occurs.
     */
    @Test
    public void testMultiNodeCacheBased() throws Exception {
        doMultiNodeTest("cache");
    }

    /**
     * @throws Exception Thrown if any exception occurs.
     */
    @Test
    public void testMultiNodeSharedFsBased() throws Exception {
        doMultiNodeTest("sharedfs");
    }

    /**
     * @throws Exception Thrown if any exception occurs.
     */
    @Test
    public void testMultiNodeDatabaseBased() throws Exception {
        doMultiNodeTest("jdbc");
    }
}
