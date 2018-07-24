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

public class TreeDataIndex {
    private final int[][] index;
    private final double[][] features;
    private final double[] labels;
    private final int rows;
    private final int cols;

    public TreeDataIndex(double[][] features, double[] labels) {
        this.features = features;
        this.labels = labels;

        rows = features.length;
        cols = features.length == 0 ? 0 : features[0].length;

        double[][] featuresCopy = new double[rows][cols];
        index = new int[rows][cols];
        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < cols; col++) {
                index[row][col] = row;
                featuresCopy[row][col] = features[row][col];
            }
        }

        for(int col = 0; col < cols; col++)
            sortIndex(featuresCopy, col, 0, rows - 1);
    }

    public double labelInSortedOrder(int k, int featureId) {
        return labels[index[k][featureId]];
    }

    public double[] featuresInSortedOrder(int k, int featureId) {
        return features[index[k][featureId]];
    }

    public double featureInSortedOrder(int k, int featureId) {
        return features[index[k][featureId]][featureId];
    }

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

                    int tmpLb = index[i][col];
                    index[i][col] = index[j][col];
                    index[j][col] = tmpLb;

                    i++;
                    j--;
                }
            }

            sortIndex(features, col, from, j);
            sortIndex(features, col, i, to);
        }
    }

    public int rowsCount() {
        return rows;
    }

    public int columnsCount() {
        return cols;
    }
}
