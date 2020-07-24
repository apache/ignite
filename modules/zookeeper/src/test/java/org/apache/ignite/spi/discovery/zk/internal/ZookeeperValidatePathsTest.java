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

package org.apache.ignite.spi.discovery.zk.internal;

import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test {@link ZkIgnitePaths#validatePath} implementation.
 */
@RunWith(Parameterized.class)
public class ZookeeperValidatePathsTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameters(name = "input string = {0}, expected error = {1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] {"/apacheIgnite", null},
            new Object[] {null, "Path cannot be null"},
            new Object[] {"", "Path length must be > 0"},
            new Object[] {"/apacheIgnite/../root", "relative paths not allowed @15"},
            new Object[] {"/apacheIgnite/./root", "relative paths not allowed @14"},
            new Object[] {"/apacheIgnite//root", "empty node name specified @14"},
            new Object[] {"/apacheIgnite\u0001", "invalid charater @13"},
            new Object[] {"/apacheIgnite/", "Path must not end with / character"}
        );
    }

    /** Zookeeper path to validate. */
    @Parameterized.Parameter(0)
    public String zkPath;

    /** Expected error. If {@code null}, path is correct. */
    @Parameterized.Parameter(1)
    public String errMsg;

    /** */
    @Test
    public void testZookeeperPathValidation() {
        try {
            ZkIgnitePaths.validatePath(zkPath);

            if (errMsg != null)
                fail(String.format("Expected failure containing \"%s\" in message did't occur", errMsg));
        }
        catch (IllegalArgumentException e) {
            if (errMsg == null) {
                fail(String.format("Path \"%s\" should be considering as valid, but failing with \"%s\"",
                    zkPath, e.getMessage()));
            }
            else {
                assertTrue(String.format("Error messages \"%s\" does't contains \"%s\"", e.getMessage(), errMsg),
                    e.getMessage().contains(errMsg));
            }
        }
    }
}
