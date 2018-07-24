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
    private final double[][] features;
    private final int[][] index;
    private final int[][] reverseIndex;
    private final double[] labels;
    private final int rows;
    private final int cols;

    public TreeDataIndex(double[][] features, double[] labels) {
        rows = features.length;
        cols = rows == 0 ? 0 : features[0].length;

        this.labels = labels;
        this.features = features;
        this.index = new int[rows][cols];
        this.reverseIndex = new int[rows][cols];

        double[][] sortedFeatures = new double[rows][cols];
        for (int i = 0; i < rows; i++) {
            for(int j = 0; j < cols; j++) {
                sortedFeatures[i][j] = features[i][j];
                index[i][j] = i;
            }
        }

        for(int i = 0; i < cols; i++)
            sortIndex(sortedFeatures, i, 0, rows - 1);

        for(int k = 0; k < rows; k++) {
            for(int col = 0; col < cols; col++) {
                int row = index[k][col];
                reverseIndex[row][col] = k;
            }
        }
    }

    public int rowsCount() {
        return rows;
    }

    public int featuresCount() {
        return cols;
    }

    public double featureInSortedOrder(int k, int featureId) {
        return features[index[k][featureId]][featureId];
    }

    public double labelInSortedOrder(int k, int featureId) {
        return labels[index[k][featureId]];
    }

    private void sortIndex(double[][] sortedFeatures, int col, int from, int to) {
        if (from < to) {
            double pivot = sortedFeatures[(from + to) / 2][col];

            int i = from, j = to;

            while (i <= j) {
                while (sortedFeatures[i][col] < pivot)
                    i++;
                while (sortedFeatures[j][col] > pivot)
                    j--;

                if (i <= j) {
                    double tmpFeature = sortedFeatures[i][col];
                    sortedFeatures[i][col] = sortedFeatures[j][col];
                    sortedFeatures[j][col] = tmpFeature;

                    int tmpLb = index[i][col];
                    index[i][col] = index[j][col];
                    index[j][col] = tmpLb;

                    i++;
                    j--;
                }
            }

            sortIndex(sortedFeatures, col, from, j);
            sortIndex(sortedFeatures, col, i, to);
        }
    }
}
