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
 * Binary classification metrics calculator.
 */
public class BinaryClassificationMetrics {
    /** Positive class label. */
    private double positiveClsLb = 1.0;

    /** Negative class label. Default value is 0.0. */
    private double negativeClsLb;

    /**
     * Calculates binary metrics values.
     *
     * @param iter Iterator that supplies pairs of truth values and predicated.
     * @return Scores for all binary metrics.
     */
    public BinaryClassificationMetricValues score(Iterator<LabelPair<Double>> iter) {
        long tp = 0;
        long tn = 0;
        long fp = 0;
        long fn = 0;

        while (iter.hasNext()) {
            LabelPair<Double> e = iter.next();

            double prediction = e.getPrediction();
            double truth = e.getTruth();

            if (prediction != negativeClsLb && prediction != positiveClsLb)
                throw new UnknownClassLabelException(prediction, positiveClsLb, negativeClsLb);
            if (truth != negativeClsLb && truth != positiveClsLb)
                throw new UnknownClassLabelException(truth, positiveClsLb, negativeClsLb);

            if (truth == positiveClsLb && prediction == positiveClsLb) tp++;
            else if (truth == positiveClsLb && prediction == negativeClsLb) fn++;
            else if (truth == negativeClsLb && prediction == negativeClsLb) tn++;
            else if (truth == negativeClsLb && prediction == positiveClsLb) fp++;
        }

        return new BinaryClassificationMetricValues(tp, tn, fp, fn);
    }

    /** */
    public double positiveClsLb() {
        return positiveClsLb;
    }

    /** */
    public BinaryClassificationMetrics withPositiveClsLb(double positiveClsLb) {
        this.positiveClsLb = positiveClsLb;
        return this;
    }

    /** */
    public double negativeClsLb() {
        return negativeClsLb;
    }

    /** */
    public BinaryClassificationMetrics withNegativeClsLb(double negativeClsLb) {
        this.negativeClsLb = negativeClsLb;
        return this;
    }
}
