/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.knn.ann;

import java.io.Serializable;
import org.apache.ignite.ml.knn.classification.KNNModelFormat;
import org.apache.ignite.ml.knn.classification.NNStrategy;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.structures.LabeledVectorSet;

/**
 * ANN model representation.
 *
 * @see ANNClassificationModel
 */
public class ANNModelFormat extends KNNModelFormat implements Serializable {
    /** Centroid statistics. */
    private final ANNClassificationTrainer.CentroidStat candidatesStat;

    /** The labeled set of candidates. */
    private LabeledVectorSet<ProbableLabel, LabeledVector> candidates;

    /**
     * Creates an instance.
     * @param k Amount of nearest neighbors.
     * @param measure Distance measure.
     * @param stgy kNN strategy.
     * @param candidatesStat The stat about candidates.
     */
    public ANNModelFormat(int k,
        DistanceMeasure measure,
        NNStrategy stgy,
        LabeledVectorSet<ProbableLabel, LabeledVector> candidates,
        ANNClassificationTrainer.CentroidStat candidatesStat) {
        this.k = k;
        this.distanceMeasure = measure;
        this.stgy = stgy;
        this.candidates = candidates;
        this.candidatesStat = candidatesStat;
    }

    /** */
    public LabeledVectorSet<ProbableLabel, LabeledVector> getCandidates() {
        return candidates;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + k;
        res = res * 37 + distanceMeasure.hashCode();
        res = res * 37 + stgy.hashCode();
        res = res * 37 + candidates.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        ANNModelFormat that = (ANNModelFormat)obj;

        return k == that.k
            && distanceMeasure.equals(that.distanceMeasure)
            && stgy.equals(that.stgy)
            && candidates.equals(that.candidates);
    }
}
