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

package org.apache.ignite.ml.tree.data;

import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link DecisionTreeData}.
 */
@RunWith(Parameterized.class)
public class DecisionTreeDataTest {
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
    public void testFilter() {
        double[][] features = new double[][]{{0}, {1}, {2}, {3}, {4}, {5}};
        double[] labels = new double[]{0, 1, 2, 3, 4, 5};

        DecisionTreeData data = new DecisionTreeData(features, labels, useIdx);
        DecisionTreeData filteredData = data.filter(obj -> obj[0] > 2);

        assertArrayEquals(new double[][]{{3}, {4}, {5}}, filteredData.getFeatures());
        assertArrayEquals(new double[]{3, 4, 5}, filteredData.getLabels(), 1e-10);
    }

    /** */
    @Test
    public void testSort() {
        double[][] features = new double[][]{{4, 1}, {3, 3}, {2, 0}, {1, 4}, {0, 2}};
        double[] labels = new double[]{0, 1, 2, 3, 4};

        DecisionTreeData data = new DecisionTreeData(features, labels, useIdx);

        data.sort(0);

        assertArrayEquals(new double[][]{{0, 2}, {1, 4}, {2, 0}, {3, 3}, {4, 1}}, features);
        assertArrayEquals(new double[]{4, 3, 2, 1, 0}, labels, 1e-10);

        data.sort(1);

        assertArrayEquals(new double[][]{{2, 0}, {4, 1}, {0, 2}, {3, 3}, {1, 4}}, features);
        assertArrayEquals(new double[]{2, 0, 4, 1, 3}, labels, 1e-10);
    }
}
