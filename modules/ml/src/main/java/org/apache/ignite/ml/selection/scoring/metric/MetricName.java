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

import org.apache.ignite.ml.selection.scoring.metric.classification.*;
import org.apache.ignite.ml.selection.scoring.metric.regression.*;

/**
 * Enum for all metrics aggregation.
 */
public enum MetricName {
    // binary classification metrics
    /** Accuracy. */
    ACCURACY("Accuracy"),

    /** Precision. */
    PRECISION("Precision"),

    /** Recall. */
    RECALL("Recall"),

    /** F measure. */
    F_MEASURE("F-measure"),

    /** TP. */
    TRUE_POSITIVE("TP"),

    /** TN. */
    TRUE_NEGATIVE("TN"),

    /** FN. */
    FALSE_NEGATIVE("FN"),

    /** FP. */
    FALSE_POSITIVE("FP"),

    /** Specificity. */
    SPECIFICITY("Specificity"),

    /** NPV. */
    NPV("NPV"),

    /** FallOut. */
    FALL_OUT("Fall out"),

    /** FDR. */
    FDR("FDR"),

    /** Miss Rate. */
    MISS_RATE("Miss rate"),

    /** Balanced accuracy. */
    BALANCED_ACCURACY("Balanced accuracy"),

    // regression metrics
    /** Mae. */
    MAE("MAE"),

    /** R 2. */
    R2("R2"),

    /** Rmse. */
    RMSE("RMSE"),

    /** Rss. */
    RSS("RSS"),

    /** Mse. */
    MSE("MSE");

    /** Pretty name. */
    private final String prettyName;

    /**
     * Creates an instance of MetricName.
     *
     * @param prettyName Pretty name.
     */
    MetricName(String prettyName) {
        this.prettyName = prettyName;
    }

    /**
     * Creates an instance of metric class by name.
     *
     * @return Metric instance.
     */
    public Metric create() {
        switch (this) {
            case ACCURACY:
                return new Accuracy();
            case PRECISION:
                return new Precision();
            case RECALL:
                return new Recall();
            case F_MEASURE:
                return new FMeasure();
            case MSE:
                return new Mse();
            case MAE:
                return new Mae();
            case R2:
                return new R2();
            case RMSE:
                return new Rmse();
            case RSS:
                return new Rss();
            case TRUE_POSITIVE:
                return new TruePositiveAbsoluteValue();
            case TRUE_NEGATIVE:
                return new TrueNegativeAbsoluteValue();
            case FALSE_POSITIVE:
                return new FalsePositiveAbsoluteValue();
            case FALSE_NEGATIVE:
                return new FalseNegativeAbsoluteValue();
            case SPECIFICITY:
                return new Specificity();
            case FALL_OUT:
                return new FallOut();
            case BALANCED_ACCURACY:
                return new BalancedAccuracy();
            case FDR:
                return new Fdr();
            case MISS_RATE:
                return new MissRate();
            case NPV:
                return new Npv();
        }

        throw new IllegalArgumentException("Cannot define metric by name: " + name());
    }

    /**
     * Returns pretty name.
     *
     * @return Name of metric.
     */
    public String getPrettyName() {
        return prettyName;
    }
}
