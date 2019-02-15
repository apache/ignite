/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.ml.tree.impurity.gini;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.tree.data.DecisionTreeData;
import org.apache.ignite.ml.tree.impurity.util.StepFunction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link GiniImpurityMeasureCalculator}.
 */
@RunWith(Parameterized.class)
public class GiniImpurityMeasureCalculatorTest {
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
        double[][] data = new double[][]{{0, 1}, {1, 0}, {2, 2}, {3, 3}};
        double[] labels = new double[]{0, 1, 1, 1};

        Map<Double, Integer> encoder = new HashMap<>();
        encoder.put(0.0, 0);
        encoder.put(1.0, 1);
        GiniImpurityMeasureCalculator calculator = new GiniImpurityMeasureCalculator(encoder, useIdx);

        StepFunction<GiniImpurityMeasure>[] impurity = calculator.calculate(new DecisionTreeData(data, labels, useIdx), fs -> true, 0);

        assertEquals(2, impurity.length);

        // Check Gini calculated for the first column.
        assertArrayEquals(new double[]{Double.NEGATIVE_INFINITY, 0, 1, 2, 3}, impurity[0].getX(), 1e-10);
        assertEquals(-2.500, impurity[0].getY()[0].impurity(), 1e-3);
        assertEquals(-4.000, impurity[0].getY()[1].impurity(),1e-3);
        assertEquals(-3.000, impurity[0].getY()[2].impurity(),1e-3);
        assertEquals(-2.666, impurity[0].getY()[3].impurity(),1e-3);
        assertEquals(-2.500, impurity[0].getY()[4].impurity(),1e-3);

        // Check Gini calculated for the second column.
        assertArrayEquals(new double[]{Double.NEGATIVE_INFINITY, 0, 1, 2, 3}, impurity[1].getX(), 1e-10);
        assertEquals(-2.500, impurity[1].getY()[0].impurity(),1e-3);
        assertEquals(-2.666, impurity[1].getY()[1].impurity(),1e-3);
        assertEquals(-3.000, impurity[1].getY()[2].impurity(),1e-3);
        assertEquals(-2.666, impurity[1].getY()[3].impurity(),1e-3);
        assertEquals(-2.500, impurity[1].getY()[4].impurity(),1e-3);
    }

    /** */
    @Test
    public void testCalculateWithRepeatedData() {
        double[][] data = new double[][]{{0}, {1}, {2}, {2}, {3}};
        double[] labels = new double[]{0, 1, 1, 1, 1};

        Map<Double, Integer> encoder = new HashMap<>();
        encoder.put(0.0, 0);
        encoder.put(1.0, 1);
        GiniImpurityMeasureCalculator calculator = new GiniImpurityMeasureCalculator(encoder, useIdx);

        StepFunction<GiniImpurityMeasure>[] impurity = calculator.calculate(new DecisionTreeData(data, labels, useIdx), fs -> true, 0);

        assertEquals(1, impurity.length);

        // Check Gini calculated for the first column.
        assertArrayEquals(new double[]{Double.NEGATIVE_INFINITY, 0, 1, 2, 3}, impurity[0].getX(), 1e-10);
        assertEquals(-3.400, impurity[0].getY()[0].impurity(), 1e-3);
        assertEquals(-5.000, impurity[0].getY()[1].impurity(),1e-3);
        assertEquals(-4.000, impurity[0].getY()[2].impurity(),1e-3);
        assertEquals(-3.500, impurity[0].getY()[3].impurity(),1e-3);
        assertEquals(-3.400, impurity[0].getY()[4].impurity(),1e-3);
    }

    /** */
    @Test
    public void testGetLabelCode() {
        Map<Double, Integer> encoder = new HashMap<>();
        encoder.put(0.0, 0);
        encoder.put(1.0, 1);
        encoder.put(2.0, 2);

        GiniImpurityMeasureCalculator calculator = new GiniImpurityMeasureCalculator(encoder, useIdx);

        assertEquals(0, calculator.getLabelCode(0.0));
        assertEquals(1, calculator.getLabelCode(1.0));
        assertEquals(2, calculator.getLabelCode(2.0));
    }
}
