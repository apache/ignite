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
    protected final boolean useIdx;

    /**
     * Constructs an instance of ImpurityMeasureCalculator.
     *
     * @param useIdx Use index.
     */
    public ImpurityMeasureCalculator(boolean useIdx) {
        this.useIdx = useIdx;
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
        return useIdx ? idx.columnsCount() : data.getFeatures()[0].length;
    }

    /**
     * Returns rows count in current dataset.
     *
     * @param data Data.
     * @param idx Index.
     * @return rows count in current dataset
     */
    protected int rowsCount(DecisionTreeData data, TreeDataIndex idx) {
        return useIdx ? idx.rowsCount() : data.getFeatures().length;
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
        return useIdx ? idx.labelInSortedOrder(k, featureId) : data.getLabels()[k];
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
        return useIdx ? idx.featureInSortedOrder(k, featureId) : data.getFeatures()[k][featureId];
    }

    protected Vector getFeatureValues(DecisionTreeData data, TreeDataIndex idx, int featureId, int k) {
        return VectorUtils.of(useIdx ? idx.featuresInSortedOrder(k, featureId) : data.getFeatures()[k]);
    }
}
