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

/**
 * A partition {@code data} of the containing matrix of features and vector of labels stored in heap.
 */
public class DecisionTreeData implements AutoCloseable {
    /** Matrix with features. */
    private final double[][] features;

    /** Vector with labels. */
    private final double[] labels;

    /**
     * Constructs a new instance of decision tree data.
     *
     * @param features Matrix with features.
     * @param labels Vector with labels.
     */
    public DecisionTreeData(double[][] features, double[] labels) {
        assert features.length == labels.length : "Features and labels have to be the same length";

        this.features = features;
        this.labels = labels;
    }

    /**
     * Filters objects and returns only data that passed filter.
     *
     * @param filter Filter.
     * @return Data passed filter.
     */
    public DecisionTreeData filter(TreeFilter filter) {
        int size = 0;

        for (int i = 0; i < features.length; i++)
            if (filter.test(features[i]))
                size++;

        double[][] newFeatures = new double[size][];
        double[] newLabels = new double[size];

        int ptr = 0;

        for (int i = 0; i < features.length; i++) {
            if (filter.test(features[i])) {
                newFeatures[ptr] = features[i];
                newLabels[ptr] = labels[i];

                ptr++;
            }
        }

        return new DecisionTreeData(newFeatures, newLabels);
    }

    /**
     * Sorts data by specified column in ascending order.
     *
     * @param col Column.
     */
    public void sort(int col) {
        sort(col, 0, features.length - 1);
    }

    /** */
    private void sort(int col, int from, int to) {
        if (from < to) {
            double pivot = features[(from + to) / 2][col];

            int i = from, j = to;

            while (i <= j) {
                while (features[i][col] < pivot) i++;
                while (features[j][col] > pivot) j--;

                if (i <= j) {
                    double[] tmpFeature = features[i];
                    features[i] = features[j];
                    features[j] = tmpFeature;

                    double tmpLb = labels[i];
                    labels[i] = labels[j];
                    labels[j] = tmpLb;

                    i++;
                    j--;
                }
            }

            sort(col, from, j);
            sort(col, i, to);
        }
    }

    /** */
    public double[][] getFeatures() {
        return features;
    }

    /** */
    public double[] getLabels() {
        return labels;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // Do nothing, GC will clean up.
    }
}
