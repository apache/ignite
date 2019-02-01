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

package org.apache.ignite.ml.inference;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for {@link IgniteModelStorageUtil}.
 */
public class IgniteModelStorageUtilTest extends GridCommonAbstractTest {
    /** Number of nodes in grid. */
    private static final int NODE_COUNT = 2;

    /** Ignite instance. */
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
//        for (int i = 1; i <= NODE_COUNT; i++)
//            startGrid(i);
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        /* Grid instance. */
//        ignite = grid(NODE_COUNT);
//        ignite.configuration().setPeerClassLoadingEnabled(true);
//        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
    }

    /** */
    @Test
    public void testSaveAndGet() {
        IgniteModel<Vector, Double> mdl = i -> 0.42;

        IgniteModelStorageUtil.saveModel(mdl, "mdl");
        Model<Vector, Double> infMdl = IgniteModelStorageUtil.getModel("mdl");

        assertEquals(0.42, infMdl.predict(VectorUtils.of()));
    }
}
