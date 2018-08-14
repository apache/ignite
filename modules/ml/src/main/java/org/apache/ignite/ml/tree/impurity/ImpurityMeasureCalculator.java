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

package org.apache.ignite.ml.tree.impurity;

import java.io.Serializable;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.tree.TreeFilter;
import org.apache.ignite.ml.tree.data.DecisionTreeData;
import org.apache.ignite.ml.tree.data.TreeDataIndex;
import org.apache.ignite.ml.tree.impurity.util.StepFunction;

/**
 * Base interface for impurity measure calculators that calculates all impurity measures required to find a best split.
 *
 * @param <T> Type of impurity measure.
 */
public abstract class ImpurityMeasureCalculator<T extends ImpurityMeasure<T>> implements Serializable {
    /** Use index structure instead of using sorting while learning. */
    protected final boolean useIndex;

    /**
     * Constructs an instance of ImpurityMeasureCalculator.
     *
     * @param useIndex Use index.
     */
    public ImpurityMeasureCalculator(boolean useIndex) {
        this.useIndex = useIndex;
    }

    /**
     * Calculates all impurity measures required required to find a best split and returns them as an array of
     * {@link StepFunction} (for every column).
     *
     * @param data Features and labels.
     * @return Impurity measures as an array of {@link StepFunction} (for every column).
     */
    public abstract StepFunction<T>[] calculate(DecisionTreeData data, TreeFilter filter, int depth);


    /**
     * Returns columns count in current dataset.
     *
     * @param data Data.
     * @param idx Index.
     * @return Columns count in current dataset.
     */
    protected int columnsCount(DecisionTreeData data, TreeDataIndex idx) {
        return useIndex ? idx.columnsCount() : data.getFeatures()[0].length;
    }

    /**
     * Returns rows count in current dataset.
     *
     * @param data Data.
     * @param idx Index.
     * @return rows count in current dataset
     */
    protected int rowsCount(DecisionTreeData data, TreeDataIndex idx) {
        return useIndex ? idx.rowsCount() : data.getFeatures().length;
    }

    /**
     * Returns label value in according to kth order statistic.
     *
     * @param data Data.
     * @param idx Index.
     * @param featureId Feature id.
     * @param k K-th statistic.
     * @return label value in according to kth order statistic
     */
    protected double getLabelValue(DecisionTreeData data, TreeDataIndex idx, int featureId, int k) {
        return useIndex ? idx.labelInSortedOrder(k, featureId) : data.getLabels()[k];
    }

    /**
     * Returns feature value in according to kth order statistic.
     *
     * @param data Data.
     * @param idx Index.
     * @param featureId Feature id.
     * @param k K-th statistic.
     * @return feature value in according to kth order statistic.
     */
    protected double getFeatureValue(DecisionTreeData data, TreeDataIndex idx, int featureId, int k) {
        return useIndex ? idx.featureInSortedOrder(k, featureId) : data.getFeatures()[k][featureId];
    }

    protected Vector getFeatureValues(DecisionTreeData data, TreeDataIndex idx, int featureId, int k) {
        return VectorUtils.of(useIndex ? idx.featuresInSortedOrder(k, featureId) : data.getFeatures()[k]);
    }
}
