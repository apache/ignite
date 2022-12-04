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

package org.apache.ignite.util;

import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/**
 * It is recommended to extend from this class in case of creating a cluster
 * for each test method. Otherwise, use
 * {@link GridCommandHandlerClusterByClassAbstractTest}
 * */
public abstract class GridCommandHandlerClusterPerMethodAbstractTest extends GridCommandHandlerAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Checks idle_vefify result.
     *
     * @param counter Counter conflicts.
     * @param hash    Hash conflicts.
     */
    protected void assertConflicts(boolean counter, boolean hash) {
        if (counter || hash)
            assertContains(log, testOut.toString(), "conflict partitions has been found: " +
                "[counterConflicts=" + (counter ? 1 : 0) + ", hashConflicts=" + (hash ? 1 : 0) + "]");
        else
            assertContains(log, testOut.toString(), "no conflicts have been found");
    }
}
