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

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import org.apache.commons.math3.util.Pair;
import org.apache.ignite.ml.selection.scoring.LabelPair;
import org.apache.ignite.ml.selection.scoring.metric.Metric;
import org.apache.ignite.ml.selection.scoring.metric.exceptions.UnknownClassLabelException;

/**
 * ROC AUC score calculator.
 * <p>
 * The calculation of AUC is based on Mann-Whitney U test
 * (https://en.wikipedia.org/wiki/Mann-Whitney_U_test).
 */
public class ROCAUC implements Metric<Double> {
    /** Positive class label. */
    private double positiveClsLb = 1.0;

    /** Negative class label. Default value is 0.0. */
    private double negativeClsLb;

    /** {@inheritDoc} */
    @Override public double score(Iterator<LabelPair<Double>> iter) {
        //TODO: It should work with not binary values only, see IGNITE-11680.

        PriorityQueue<Pair<Double, Double>> queue = new PriorityQueue<>(Comparator.comparingDouble(Pair::getKey));

        long pos = 0;
        long neg = 0;

        while (iter.hasNext()) {
            LabelPair<Double> e = iter.next();

            Double prediction = e.getPrediction();
            Double truth = e.getTruth();

            queue.add(new Pair<>(prediction, truth));

            if (truth == positiveClsLb)
                pos++;
            else if (truth == negativeClsLb)
                neg++;
            else
                throw new UnknownClassLabelException(truth, positiveClsLb, negativeClsLb);

        }

        return calculateROCAUC(queue, pos, neg, positiveClsLb);
    }

    /**
     * Calculates the ROC AUC value based on queue of pairs,
     * amount of positive/negative cases and label of positive class.
     */
    public static double calculateROCAUC(PriorityQueue<Pair<Double, Double>> queue, long pos, long neg, double positiveClsLb) {
        double[] lb = new double[queue.size()];
        double[] prediction = new double[queue.size()];
        int cnt = 0;

        while (!queue.isEmpty()) {
            Pair<Double, Double> elem = queue.poll();
            lb[cnt] = elem.getValue();
            prediction[cnt] = elem.getKey();
            cnt++;
        }

        double[] rank = new double[lb.length];
        for (int i = 0; i < prediction.length; i++) {
            if (i == prediction.length - 1 || prediction[i] != prediction[i + 1])
                rank[i] = i + 1;
            else {
                int j = i + 1;
                for (; j < prediction.length && prediction[j] == prediction[i]; j++);
                double r = (i + 1 + j) / 2.0;
                for (int k = i; k < j; k++)
                    rank[k] = r;
                i = j - 1;
            }
        }

        double auc = 0.0;
        for (int i = 0; i < lb.length; i++) {
            if (lb[i] == positiveClsLb)
                auc += rank[i];
        }

        if (pos == 0L) return Double.NaN;
        else if (neg == 0L) return Double.NaN;

        auc = (auc - (pos * (pos + 1) / 2.0)) / (pos * neg);
        return auc;
    }

    /** Get the positive label. */
    public double positiveClsLb() {
        return positiveClsLb;
    }

    /** Set the positive label. */
    public ROCAUC withPositiveClsLb(double positiveClsLb) {
        if (Double.isFinite(positiveClsLb))
            this.positiveClsLb = positiveClsLb;
        return this;
    }

    /** Get the negative label. */
    public double negativeClsLb() {
        return negativeClsLb;
    }

    /** Set the negative label. */
    public ROCAUC withNegativeClsLb(double negativeClsLb) {
        if (Double.isFinite(negativeClsLb))
            this.negativeClsLb = negativeClsLb;
        return this;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "ROC AUC";
    }
}
