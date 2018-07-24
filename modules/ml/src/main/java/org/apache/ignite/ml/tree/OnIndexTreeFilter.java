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

package org.apache.ignite.ml.tree;

import java.util.HashSet;
import java.util.Set;
import java.util.function.BiPredicate;

public class OnIndexTreeFilter {
    private Set<Integer> filteredRows;

    public OnIndexTreeFilter() {
        filteredRows = new HashSet<>();
    }

    private OnIndexTreeFilter(Set<Integer> filteredRows) {
        this.filteredRows = filteredRows;
    }

    public boolean apply(int rowId) {
        return filteredRows.contains(rowId);
    }

    public OnIndexTreeFilter lessThan(int columnId, double value, double[][] sortedFeatures, int[][] index) {
        return buildFilter(columnId, value, sortedFeatures, index, (feature, threshold) -> feature <= threshold);
    }

    public OnIndexTreeFilter greaterThan(int columnId, double value, double[][] sortedFeatures, int[][] index) {
        return buildFilter(columnId, value, sortedFeatures, index, (feature, threshold) -> feature > threshold);
    }

    private OnIndexTreeFilter buildFilter(int columnId, double value, double[][] sortedFeatures,
        int[][] index, BiPredicate<Double, Double> predicate) {

        Set<Integer> newRows = new HashSet<>(filteredRows);
        for(int i = 0; i < sortedFeatures.length; i++) {
            if(predicate.test(sortedFeatures[i][columnId], value))
                newRows.add(index[i][columnId]);
        }
        return new OnIndexTreeFilter(newRows);
    }
}
