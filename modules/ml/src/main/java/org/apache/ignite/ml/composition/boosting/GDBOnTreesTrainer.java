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

package org.apache.ignite.ml.composition.boosting;

import org.apache.ignite.ml.tree.DecisionTreeRegressionTrainer;
import org.jetbrains.annotations.NotNull;

public abstract class GDBOnTreesTrainer extends GDBTrainer {
    private final int maxDepth;
    private final double minImpurityDecrease;

    private GDBOnTreesTrainer(double gradStepSize,
        Integer modelsCnt,
        int maxDepth,
        double minImpurityDecrease) {

        super(gradStepSize, modelsCnt);
        this.maxDepth = maxDepth;
        this.minImpurityDecrease = minImpurityDecrease;
    };

    @NotNull @Override protected DecisionTreeRegressionTrainer buildBaseModelTrainer() {
        return new DecisionTreeRegressionTrainer(maxDepth, minImpurityDecrease);
    }

    public static GDBTrainer regression(double gradStepSize,
        Integer modelsCnt,
        int maxDepth,
        double minImpurityDecrease) {

        return new GDBRegressionTrainer(gradStepSize, modelsCnt) {
            @NotNull @Override protected DecisionTreeRegressionTrainer buildBaseModelTrainer() {
                return new DecisionTreeRegressionTrainer(maxDepth, minImpurityDecrease);
            }
        };
    }

    public static GDBTrainer binaryClassification(double gradStepSize,
        Integer modelsCnt,
        int maxDepth,
        double minImpurityDecrease) {

        return new GDBBinaryClassifierTrainer(gradStepSize, modelsCnt) {
            @NotNull @Override protected DecisionTreeRegressionTrainer buildBaseModelTrainer() {
                return new DecisionTreeRegressionTrainer(maxDepth, minImpurityDecrease);
            }
        };
    }
}
