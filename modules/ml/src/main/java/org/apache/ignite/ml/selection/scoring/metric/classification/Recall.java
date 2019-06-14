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

package org.apache.ignite.ml.selection.scoring.metric.classification;

import org.apache.ignite.ml.selection.scoring.LabelPair;

import java.util.Iterator;

/**
 * Recall calculator.
 *
 * @param <L> Type of a label (truth or prediction).
 */
public class Recall<L> extends ClassMetric<L> {
    /**
     * The class of interest or positive class.
     *
     * @param clsLb The label.
     */
    public Recall(L clsLb) {
        super(clsLb);
    }

    /** {@inheritDoc} */
    @Override public double score(Iterator<LabelPair<L>> it) {
        if (clsLb != null) {
            long tp = 0;
            long fn = 0;

            while (it.hasNext()) {
                LabelPair<L> e = it.next();

                L prediction = e.getPrediction();
                L truth = e.getTruth();

                if (clsLb.equals(truth)) {
                    if (prediction.equals(truth))
                        tp++;
                    else
                        fn++;
                }
            }
            long denominator = tp + fn;

            if (denominator == 0)
                return 1; // according to https://github.com/dice-group/gerbil/wiki/Precision,-Recall-and-F1-measure

            return (double)tp / denominator;
        }
        else
            return Double.NaN;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "recall for class with label " + clsLb;
    }
}
