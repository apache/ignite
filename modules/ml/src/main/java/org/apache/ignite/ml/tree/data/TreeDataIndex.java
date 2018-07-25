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

import java.util.BitSet;
import org.apache.ignite.ml.tree.TreeFilter;

public class TreeDataIndex {
    private final int[][] index;
    private final int[][] indexProj;
    private final double[][] features;
    private final double[] labels;
    private final BitSet hasOriginalRows;

    public TreeDataIndex(double[][] features, double[] labels) {
        this.features = features;
        this.labels = labels;

        int rows = features.length;
        int cols = features.length == 0 ? 0 : features[0].length;

        double[][] featuresCopy = new double[rows][cols];
        index = new int[rows][cols];
        hasOriginalRows = new BitSet();
        for (int row = 0; row < rows; row++) {
            hasOriginalRows.set(row);
            for (int col = 0; col < cols; col++) {
                index[row][col] = row;
                featuresCopy[row][col] = features[row][col];
            }
        }

        for (int col = 0; col < cols; col++)
            sortIndex(featuresCopy, col, 0, rows - 1);

        indexProj = index;
    }

    public TreeDataIndex(int[][] index, int[][] indexProj, BitSet hasRows, double[][] features,
        double[] labels) {
        this.index = index;
        this.indexProj = indexProj;
        this.features = features;
        this.labels = labels;
        this.hasOriginalRows = hasRows;
    }

    public double labelInSortedOrder(int k, int featureId) {
        return labels[indexProj[k][featureId]];
    }

    public double[] featuresInSortedOrder(int k, int featureId) {
        return features[indexProj[k][featureId]];
    }

    public double featureInSortedOrder(int k, int featureId) {
        return features[indexProj[k][featureId]][featureId];
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

    public TreeDataIndex filter(TreeFilter filter) {
        BitSet filteredRows = new BitSet();
        int projSize = 0;
        for (int i = 0; i < rowsCount(); i++) {
            if (filter.test(featuresInSortedOrder(i, 0))) {
                filteredRows.set(indexProj[i][0]);
                projSize++;
            }
        }

        int[][] newIndexProj = new int[projSize][columnsCount()];
        for(int feature = 0; feature < columnsCount(); feature++) {
            int ptr = 0;
            for(int row = 0; row < rowsCount(); row++) {
                if(filteredRows.get(indexProj[row][feature]))
                    newIndexProj[ptr++][feature] = indexProj[row][feature];
            }
        }

        return new TreeDataIndex(index, newIndexProj, filteredRows, features, labels);
    }

    public int rowsCount() {
        return indexProj.length;
    }

    public int columnsCount() {
        return rowsCount() == 0 ? 0 : indexProj[0].length ;
    }
}
