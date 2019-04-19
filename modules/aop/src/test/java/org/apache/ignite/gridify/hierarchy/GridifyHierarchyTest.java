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

package org.apache.ignite.gridify.hierarchy;

import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Gridify hierarchy test.
 */
@RunWith(JUnit4.class)
public class GridifyHierarchyTest extends GridCommonAbstractTest {
    /** */
    public GridifyHierarchyTest() {
        super(true);
    }

    /** */
    public void noneTestGridifyHierarchyProtected() {
        Target target = new Target();

        target.methodA();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGridifyHierarchyPrivate() throws Exception {
        Target target = new Target();

        target.methodB();
    }

   /** {@inheritDoc} */
    @Override public String getTestIgniteInstanceName() {
        return "GridifyHierarchyTest";
    }
}
