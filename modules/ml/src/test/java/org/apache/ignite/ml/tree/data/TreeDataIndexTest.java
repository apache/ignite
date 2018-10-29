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

package org.apache.ignite.ml.tree.data;

import org.apache.ignite.ml.tree.TreeFilter;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Test for {@link TreeDataIndex}.
 */
public class TreeDataIndexTest {
    /**  */
    private double[][] features = {
        {1., 2., 3., 4.},
        {2., 3., 4., 1.},
        {3., 4., 1., 2.},
        {4., 1., 2., 3.}
    };

    /** */
    private double[] labels = {1., 2., 3, 4.};

    /** */
    private double[][] labelsInSortedOrder = {
        {1., 4., 3., 2.},
        {2., 1., 4., 3.},
        {3., 2., 1., 4.},
        {4., 3., 2., 1.}
    };

    /** */
    private double[][][] featuresInSortedOrder = {
        {
            {1., 2., 3., 4.},
            {4., 1., 2., 3.},
            {3., 4., 1., 2.},
            {2., 3., 4., 1.},
        },
        {
            {2., 3., 4., 1.},
            {1., 2., 3., 4.},
            {4., 1., 2., 3.},
            {3., 4., 1., 2.},
        },
        {
            {3., 4., 1., 2.},
            {2., 3., 4., 1.},
            {1., 2., 3., 4.},
            {4., 1., 2., 3.},
        },
        {
            {4., 1., 2., 3.},
            {3., 4., 1., 2.},
            {2., 3., 4., 1.},
            {1., 2., 3., 4.},
        }
    };

    /** */
    private TreeDataIndex idx = new TreeDataIndex(features, labels);

    /** */
    @Test
    public void labelInSortedOrderTest() {
        assertEquals(features.length, idx.rowsCount());
        assertEquals(features[0].length, idx.columnsCount());

        for (int k = 0; k < idx.rowsCount(); k++) {
            for (int featureId = 0; featureId < idx.columnsCount(); featureId++)
                assertEquals(labelsInSortedOrder[k][featureId], idx.labelInSortedOrder(k, featureId), 0.01);
        }
    }

    /** */
    @Test
    public void featuresInSortedOrderTest() {
        assertEquals(features.length, idx.rowsCount());
        assertEquals(features[0].length, idx.columnsCount());

        for (int k = 0; k < idx.rowsCount(); k++) {
            for (int featureId = 0; featureId < idx.columnsCount(); featureId++)
                assertArrayEquals(featuresInSortedOrder[k][featureId], idx.featuresInSortedOrder(k, featureId), 0.01);
        }
    }

    /** */
    @Test
    public void featureInSortedOrderTest() {
        assertEquals(features.length, idx.rowsCount());
        assertEquals(features[0].length, idx.columnsCount());

        for (int k = 0; k < idx.rowsCount(); k++) {
            for (int featureId = 0; featureId < idx.columnsCount(); featureId++)
                assertEquals((double)k + 1, idx.featureInSortedOrder(k, featureId), 0.01);
        }
    }

    /** */
    @Test
    public void filterTest() {
        TreeFilter filter1 = features -> features[0] > 2;
        TreeFilter filter2 = features -> features[1] > 2;
        TreeFilter filterAnd = filter1.and(features -> features[1] > 2);

        TreeDataIndex filtered1 = idx.filter(filter1);
        TreeDataIndex filtered2 = filtered1.filter(filter2);
        TreeDataIndex filtered3 = idx.filter(filterAnd);

        assertEquals(2, filtered1.rowsCount());
        assertEquals(4, filtered1.columnsCount());
        assertEquals(1, filtered2.rowsCount());
        assertEquals(4, filtered2.columnsCount());
        assertEquals(1, filtered3.rowsCount());
        assertEquals(4, filtered3.columnsCount());

        double[] obj1 = {3, 4, 1, 2};
        double[] obj2 = {4, 1, 2, 3};
        double[][] restObjs = new double[][] {obj1, obj2};
        int[][] restObjIndxInSortedOrderPerFeatures = new int[][] {
            {0, 1}, //feature 0
            {1, 0}, //feature 1
            {0, 1}, //feature 2
            {0, 1}, //feature 3
        };

        for (int featureId = 0; featureId < filtered1.columnsCount(); featureId++) {
            for (int k = 0; k < filtered1.rowsCount(); k++) {
                int objId = restObjIndxInSortedOrderPerFeatures[featureId][k];
                double[] obj = restObjs[objId];
                assertArrayEquals(obj, filtered1.featuresInSortedOrder(k, featureId), 0.01);
            }
        }

        for (int featureId = 0; featureId < filtered2.columnsCount(); featureId++) {
            for (int k = 0; k < filtered2.rowsCount(); k++) {
                assertArrayEquals(obj1, filtered2.featuresInSortedOrder(k, featureId), 0.01);
                assertArrayEquals(obj1, filtered3.featuresInSortedOrder(k, featureId), 0.01);
            }
        }
    }
}
