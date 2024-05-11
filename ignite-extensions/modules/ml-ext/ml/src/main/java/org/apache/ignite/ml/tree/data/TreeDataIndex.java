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

import java.util.Arrays;
import org.apache.ignite.ml.tree.TreeFilter;

/**
 * Index for representing sorted dataset rows for each features.
 * It may be reused while decision tree learning at several levels through filter method.
 */
public class TreeDataIndex {
    /** Index containing IDs of rows as if they is sorted by feature values. */
    private final int[][] idx;

    /** Original features table. */
    private final double[][] features;

    /** Original labels. */
    private final double[] labels;

    /**
     * Constructs an instance of TreeDataIndex.
     *
     * @param features Features.
     * @param labels Labels.
     */
    public TreeDataIndex(double[][] features, double[] labels) {
        this.features = features;
        this.labels = labels;

        int rows = features.length;
        int cols = features.length == 0 ? 0 : features[0].length;

        double[][] featuresCp = new double[rows][cols];
        idx = new int[rows][cols];
        for (int row = 0; row < rows; row++) {
            Arrays.fill(idx[row], row);
            featuresCp[row] = Arrays.copyOf(features[row], cols);
        }

        for (int col = 0; col < cols; col++)
            sortIndex(featuresCp, col, 0, rows - 1);
    }

    /**
     * Constructs an instance of TreeDataIndex
     *
     * @param idxProj Index projection.
     * @param features Features.
     * @param labels Labels.
     */
    private TreeDataIndex(int[][] idxProj, double[][] features, double[] labels) {
        this.idx = idxProj;
        this.features = features;
        this.labels = labels;
    }

    /**
     * Returns label for kth order statistic for target feature.
     *
     * @param k K.
     * @param featureId Feature id.
     * @return Label value.
     */
    public double labelInSortedOrder(int k, int featureId) {
        return labels[idx[k][featureId]];
    }

    /**
     * Returns vector of original features for kth order statistic for target feature.
     *
     * @param k K.
     * @param featureId Feature id.
     * @return Features vector.
     */
    public double[] featuresInSortedOrder(int k, int featureId) {
        return features[idx[k][featureId]];
    }

    /**
     * Returns feature value for kth order statistic for target feature.
     *
     * @param k K.
     * @param featureId Feature id.
     * @return Feature value.
     */
    public double featureInSortedOrder(int k, int featureId) {
        return featuresInSortedOrder(k, featureId)[featureId];
    }

    /**
     * Creates projection of current index in according to {@link TreeFilter}.
     *
     * @param filter Filter.
     * @return Projection of current index onto smaller index in according to rows filter.
     */
    public TreeDataIndex filter(TreeFilter filter) {
        int projSize = 0;
        for (int i = 0; i < rowsCount(); i++) {
            if (filter.test(featuresInSortedOrder(i, 0)))
                projSize++;
        }

        int[][] prj = new int[projSize][columnsCount()];
        for (int feature = 0; feature < columnsCount(); feature++) {
            int ptr = 0;
            for (int row = 0; row < rowsCount(); row++) {
                if (filter.test(featuresInSortedOrder(row, feature)))
                    prj[ptr++][feature] = idx[row][feature];
            }
        }

        return new TreeDataIndex(prj, features, labels);
    }

    /**
     * @return count of rows in current index.
     */
    public int rowsCount() {
        return idx.length;
    }

    /**
     * @return count of columns in current index.
     */
    public int columnsCount() {
        return rowsCount() == 0 ? 0 : idx[0].length;
    }

    /**
     * Constructs index structure in according to features table.
     *
     * @param features Features.
     * @param col Column.
     * @param from From.
     * @param to To.
     */
    private void sortIndex(double[][] features, int col, int from, int to) {
        if (from < to) {
            double pivot = features[(from + to) / 2][col];

            int i = from, j = to;

            while (i <= j) {
                while (features[i][col] < pivot)
                    i++;
                while (features[j][col] > pivot)
                    j--;

                if (i <= j) {
                    double tmpFeature = features[i][col];
                    features[i][col] = features[j][col];
                    features[j][col] = tmpFeature;

                    int tmpLb = idx[i][col];
                    idx[i][col] = idx[j][col];
                    idx[j][col] = tmpLb;

                    i++;
                    j--;
                }
            }

            sortIndex(features, col, from, j);
            sortIndex(features, col, i, to);
        }
    }
}
