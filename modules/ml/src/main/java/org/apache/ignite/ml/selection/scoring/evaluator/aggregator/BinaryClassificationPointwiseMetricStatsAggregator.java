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

package org.apache.ignite.ml.selection.scoring.evaluator.aggregator;

import java.io.Serializable;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.selection.scoring.evaluator.context.BinaryClassificationEvaluationContext;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * This class represents statistics for pointwise metrics evaluation for binary classification like TruePositive,
 * FalsePositive, TrueNegative and FalseNegative.
 */
public class BinaryClassificationPointwiseMetricStatsAggregator<L extends Serializable> implements MetricStatsAggregator<L, BinaryClassificationEvaluationContext<L>, BinaryClassificationPointwiseMetricStatsAggregator<L>> {
    /**
     * Serial version uid.
     */
    private static final long serialVersionUID = -7677193556950322385L;

    /**
     * False label.
     */
    private L falseLabel;

    /**
     * Truth label.
     */
    private L truthLabel;

    /**
     * Amount of true positives.
     */
    private int truePositive;

    /**
     * Amount of false positives.
     */
    int falsePositive;

    /**
     * Amount of true negatives.
     */
    int trueNegative;

    /**
     * Amount of false negatives.
     */
    int falseNegative;

    /**
     * Creates an instance of BinaryClassificationPointwiseMetricStatsAggregator.
     */
    public BinaryClassificationPointwiseMetricStatsAggregator() {
    }

    /**
     * Creates an instance of BinaryClassificationPointwiseMetricStatsAggregator.
     *
     * @param falseLabel    False label.
     * @param truthLabel    Truth label.
     * @param truePositive  True positives amount.
     * @param falsePositive False positives amount.
     * @param trueNegative  True negatives amount.
     * @param falseNegative False negatives amount.
     */
    public BinaryClassificationPointwiseMetricStatsAggregator(L falseLabel, L truthLabel,
        int truePositive, int falsePositive, int trueNegative, int falseNegative) {

        this.falseLabel = falseLabel;
        this.truthLabel = truthLabel;
        this.truePositive = truePositive;
        this.falsePositive = falsePositive;
        this.trueNegative = trueNegative;
        this.falseNegative = falseNegative;
    }

    /**
     * {@inheritDoc}
     */
    @Override public void aggregate(IgniteModel<Vector, L> mdl, LabeledVector<L> vector) {
        L modelAns = mdl.predict(vector.features());
        L realAns = vector.label();

        if (modelAns.equals(falseLabel) && realAns.equals(falseLabel))
            trueNegative += 1;
        else if (modelAns.equals(falseLabel) && realAns.equals(truthLabel))
            falseNegative += 1;
        else if (modelAns.equals(truthLabel) && realAns.equals(truthLabel))
            truePositive += 1;
        else if (modelAns.equals(truthLabel) && realAns.equals(falseLabel))
            falsePositive += 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override public BinaryClassificationPointwiseMetricStatsAggregator<L> mergeWith(
        BinaryClassificationPointwiseMetricStatsAggregator other) {
        A.ensure(this.falseLabel.equals(other.falseLabel), "this.falseLabel == other.falseLabel");
        A.ensure(this.truthLabel.equals(other.truthLabel), "this.truthLabel == other.truthLabel");

        return new BinaryClassificationPointwiseMetricStatsAggregator<>(
            this.falseLabel,
            this.truthLabel,
            this.truePositive + other.truePositive,
            this.falsePositive + other.falsePositive,
            this.trueNegative + other.trueNegative,
            this.falseNegative + other.falseNegative
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override public BinaryClassificationEvaluationContext<L> createInitializedContext() {
        return new BinaryClassificationEvaluationContext<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override public void initByContext(BinaryClassificationEvaluationContext<L> ctx) {
        this.falseLabel = ctx.getFirstClsLbl();
        this.truthLabel = ctx.getSecondClsLbl();
    }

    /**
     * Returns false label.
     *
     * @return False label.
     */
    public L getFalseLabel() {
        return falseLabel;
    }

    /**
     * Returns truth label.
     *
     * @return Truth label.
     */
    public L getTruthLabel() {
        return truthLabel;
    }

    /**
     * Returns true positives count.
     *
     * @return True positives count.
     */
    public int getTruePositive() {
        return truePositive;
    }

    /**
     * Returns false positives count.
     *
     * @return False positives count.
     */
    public int getFalsePositive() {
        return falsePositive;
    }

    /**
     * Returns true negatives count.
     *
     * @return True negatives count.
     */
    public int getTrueNegative() {
        return trueNegative;
    }

    /**
     * Returns false negatives count.
     *
     * @return False negatives count.
     */
    public int getFalseNegative() {
        return falseNegative;
    }

    /**
     * Returns number of elements in dataset.
     *
     * @return Number of elements in dataset.
     */
    public int getN() {
        return truePositive + falsePositive + trueNegative + falseNegative;
    }

    /**
     * Class represents already initialized aggregator.
     */
    public static class WithCustomLabelsAggregator<L extends Serializable> extends BinaryClassificationPointwiseMetricStatsAggregator<L> {
        /**
         * Truth label.
         */
        private final L truthLabel;

        /**
         * False label.
         */
        private final L falseLabel;

        /**
         * Create an instance of WithCustomLabels.
         *
         * @param truthLabel Truth label.
         * @param falseLabel False label.
         */
        public WithCustomLabelsAggregator(L truthLabel, L falseLabel) {
            this.truthLabel = truthLabel;
            this.falseLabel = falseLabel;
        }

        /**
         * {@inheritDoc}
         */
        @Override public BinaryClassificationEvaluationContext<L> createInitializedContext() {
            return new BinaryClassificationEvaluationContext<L>(falseLabel, truthLabel) {
                /**
                 * Serial version ID.
                 */
                private static final long serialVersionUID = 4739649114414953828L;

                /** {@inheritDoc} */
                @Override public boolean needToCompute() {
                    return false;
                }
            };
        }
    }
}
