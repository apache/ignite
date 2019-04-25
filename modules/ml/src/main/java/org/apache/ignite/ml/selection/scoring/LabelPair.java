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

package org.apache.ignite.ml.selection.scoring;

/**
 * Pair of truth value and predicated by model.
 *
 * @param <L> Type of a label (truth or prediction).
 */
public class LabelPair<L> {
    /** Truth value. */
    private final L truth;

    /** Predicted value. */
    private final L prediction;

    /**
     * Constructs a new instance of truth with prediction.
     *
     * @param truth Truth value.
     * @param prediction Predicted value.
     */
    public LabelPair(L truth, L prediction) {
        this.truth = truth;
        this.prediction = prediction;
    }

    /** */
    public L getTruth() {
        return truth;
    }

    /** */
    public L getPrediction() {
        return prediction;
    }
}
