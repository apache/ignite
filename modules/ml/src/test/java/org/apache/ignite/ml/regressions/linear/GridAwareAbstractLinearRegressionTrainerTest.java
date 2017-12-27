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

package org.apache.ignite.ml.regressions.linear;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public abstract class GridAwareAbstractLinearRegressionTrainerTest extends GridCommonAbstractTest {
    /** Number of nodes in grid */
    private static final int NODE_COUNT = 2;

    /**
     * Delegate actually performs tests.
     */
    private final GenericLinearRegressionTrainerTest delegate;

    /** */
    private Ignite ignite;

    /** */
    public GridAwareAbstractLinearRegressionTrainerTest(
        Trainer<LinearRegressionModel, Matrix> trainer,
        IgniteFunction<double[][], Matrix> matrixCreator,
        IgniteFunction<double[], Vector> vectorCreator,
        double precision) {
        delegate = new GenericLinearRegressionTrainerTest(trainer, matrixCreator, vectorCreator, precision);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 1; i <= NODE_COUNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() {
        stopAllGrids();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        /* Grid instance. */
        ignite = grid(NODE_COUNT);
        ignite.configuration().setPeerClassLoadingEnabled(true);
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
    }

    /**
     * Test trainer on regression model y = 2 * x.
     */
    @Test
    public void testTrainWithoutIntercept() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        delegate.testTrainWithoutIntercept();
    }

    /**
     * Test trainer on regression model y = -1 * x + 1.
     */
    @Test
    public void testTrainWithIntercept() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        delegate.testTrainWithIntercept();
    }

    /**
     * Tests trainer on artificial dataset with 10 observations described by 1 feature.
     */
    @Test
    public void testTrainOnArtificialDataset10x1() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        delegate.testTrainOnArtificialDataset10x1();
    }

    /**
     * Tests trainer on artificial dataset with 10 observations described by 5 features.
     */
    @Test
    public void testTrainOnArtificialDataset10x5() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        delegate.testTrainOnArtificialDataset10x5();
    }

    /**
     * Tests trainer on artificial dataset with 100 observations described by 5 features.
     */
    @Test
    public void testTrainOnArtificialDataset100x5() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        delegate.testTrainOnArtificialDataset100x5();
    }

    /**
     * Tests trainer on artificial dataset with 100 observations described by 10 features.
     */
    @Test
    public void testTrainOnArtificialDataset100x10() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        delegate.testTrainOnArtificialDataset100x10();
    }
}
