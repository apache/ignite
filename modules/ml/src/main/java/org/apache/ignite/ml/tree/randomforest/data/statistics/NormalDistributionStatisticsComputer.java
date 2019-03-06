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

package org.apache.ignite.ml.tree.randomforest.data.statistics;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.feature.FeatureMeta;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedDatasetPartition;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Normal distribution parameters computer logic.
 */
public class NormalDistributionStatisticsComputer implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = -3699071003012595743L;

    /**
     * Computes statistics of normal distribution on features in dataset.
     *
     * @param meta Meta.
     * @param dataset Dataset.
     */
    public List<NormalDistributionStatistics> computeStatistics(List<FeatureMeta> meta, Dataset<EmptyContext,
        BootstrappedDatasetPartition> dataset) {

        return dataset.compute(
            x -> computeStatsOnPartition(x, meta),
            (l, r) -> reduceStats(l, r, meta)
        );
    }

    /**
     * Aggregates normal distribution statistics for continual features in dataset partition.
     *
     * @param part Partition.
     * @param meta Meta.
     * @return Statistics for each feature.
     */
    public List<NormalDistributionStatistics> computeStatsOnPartition(BootstrappedDatasetPartition part,
        List<FeatureMeta> meta) {

        double[] sumOfValues = new double[meta.size()];
        double[] sumOfSquares = new double[sumOfValues.length];
        double[] min = new double[sumOfValues.length];
        double[] max = new double[sumOfValues.length];
        Arrays.fill(min, Double.POSITIVE_INFINITY);
        Arrays.fill(max, Double.NEGATIVE_INFINITY);

        for (int i = 0; i < part.getRowsCount(); i++) {
            Vector vec = part.getRow(i).features();
            for (int featureId = 0; featureId < vec.size(); featureId++) {
                if (!meta.get(featureId).isCategoricalFeature()) {
                    double featureVal = vec.get(featureId);
                    sumOfValues[featureId] += featureVal;
                    sumOfSquares[featureId] += Math.pow(featureVal, 2);
                    min[featureId] = Math.min(min[featureId], featureVal);
                    max[featureId] = Math.max(max[featureId], featureVal);
                }
            }
        }

        ArrayList<NormalDistributionStatistics> res = new ArrayList<>();
        for (int featureId = 0; featureId < sumOfSquares.length; featureId++) {
            res.add(new NormalDistributionStatistics(
                min[featureId], max[featureId],
                sumOfSquares[featureId], sumOfValues[featureId],
                part.getRowsCount())
            );
        }
        return res;
    }

    /**
     * Merges statistics on features from two partitions.
     *
     * @param left Left.
     * @param right Right.
     * @param meta Features meta.
     * @return plus of statistics for each features.
     */
    public List<NormalDistributionStatistics> reduceStats(List<NormalDistributionStatistics> left,
        List<NormalDistributionStatistics> right,
        List<FeatureMeta> meta) {

        if (left == null)
            return right;
        if (right == null)
            return left;

        assert meta.size() == left.size() && meta.size() == right.size();
        List<NormalDistributionStatistics> res = new ArrayList<>();
        for (int featureId = 0; featureId < meta.size(); featureId++) {
            NormalDistributionStatistics leftStat = left.get(featureId);
            NormalDistributionStatistics rightStat = right.get(featureId);
            res.add(leftStat.plus(rightStat));
        }
        return res;
    }
}
