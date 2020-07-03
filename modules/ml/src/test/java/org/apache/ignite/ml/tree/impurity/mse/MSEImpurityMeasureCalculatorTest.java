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

package org.apache.ignite.ml.tree.impurity.mse;

import java.util.Arrays;
import org.apache.ignite.ml.tree.data.DecisionTreeData;
import org.apache.ignite.ml.tree.impurity.util.StepFunction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link MSEImpurityMeasureCalculator}.
 */
@RunWith(Parameterized.class)
public class MSEImpurityMeasureCalculatorTest {
    /** Parameters. */
    @Parameterized.Parameters(name = "Use index {0}")
    public static Iterable<Boolean[]> data() {
        return Arrays.asList(
            new Boolean[] {true},
            new Boolean[] {false}
        );
    }

    /** Use index. */
    @Parameterized.Parameter
    public boolean useIdx;

    /** */
    @Test
    public void testCalculate() {
        double[][] data = new double[][]{{0, 2}, {1, 1}, {2, 0}, {3, 3}};
        double[] labels = new double[]{1, 2, 2, 1};

        MSEImpurityMeasureCalculator calculator = new MSEImpurityMeasureCalculator(useIdx);

        StepFunction<MSEImpurityMeasure>[] impurity = calculator.calculate(new DecisionTreeData(data, labels, useIdx), fs -> true, 0);

        assertEquals(2, impurity.length);

        // Test MSE calculated for the first column.
        assertArrayEquals(new double[]{Double.NEGATIVE_INFINITY, 0, 1, 2, 3}, impurity[0].getX(), 1e-10);
        assertEquals(1.000, impurity[0].getY()[0].impurity(), 1e-3);
        assertEquals(0.666, impurity[0].getY()[1].impurity(),1e-3);
        assertEquals(1.000, impurity[0].getY()[2].impurity(),1e-3);
        assertEquals(0.666, impurity[0].getY()[3].impurity(),1e-3);
        assertEquals(1.000, impurity[0].getY()[4].impurity(),1e-3);

        // Test MSE calculated for the second column.
        assertArrayEquals(new double[]{Double.NEGATIVE_INFINITY, 0, 1, 2, 3}, impurity[1].getX(), 1e-10);
        assertEquals(1.000, impurity[1].getY()[0].impurity(),1e-3);
        assertEquals(0.666, impurity[1].getY()[1].impurity(),1e-3);
        assertEquals(0.000, impurity[1].getY()[2].impurity(),1e-3);
        assertEquals(0.666, impurity[1].getY()[3].impurity(),1e-3);
        assertEquals(1.000, impurity[1].getY()[4].impurity(),1e-3);
    }
}
