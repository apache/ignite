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

package org.apache.ignite.internal.managers.checkpoint;

import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

/**
 *
 */
@GridCommonTest(group = "Checkpoint Manager")
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
