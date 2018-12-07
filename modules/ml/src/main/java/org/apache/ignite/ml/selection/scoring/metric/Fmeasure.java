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

package org.apache.ignite.ml.selection.scoring.metric;

import java.util.Iterator;
import org.apache.ignite.ml.selection.scoring.LabelPair;

/**
 * F-measure calculator.
 *
 * @param <L> Type of a label (truth or prediction).
 */
public class Fmeasure<L> implements Metric<L> {
    /** Class label. */
    private L clsLb;

    /** {@inheritDoc} */
    @Override public double score(Iterator<LabelPair<L>> it) {
        if (clsLb != null) {
            long tp = 0;
            long fn = 0;
            long fp = 0;
            double recall;
            double precision;

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

                if (clsLb.equals(prediction) && !prediction.equals(truth))
                    fp++;
            }
            long denominatorRecall = tp + fn;

            long denominatorPrecision = tp + fp;

            // according to https://github.com/dice-group/gerbil/wiki/Precision,-Recall-and-F1-measure
            recall = denominatorRecall == 0 ? 1 : (double)tp / denominatorRecall;
            precision = denominatorPrecision == 0 ? 1 : (double)tp / denominatorPrecision;

            return 2 * (recall * precision) / (recall + precision);
        }
        else
            return Double.NaN;
    }

    /**
     * The class of interest or positive class.
     *
     * @param clsLb The label.
     */
    public Fmeasure(L clsLb) {
        this.clsLb = clsLb;
    }
}
