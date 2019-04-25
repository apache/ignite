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

package org.apache.ignite.ml.composition.boosting.loss;

/**
 * Logistic regression loss function.
 */
public class LogLoss implements Loss {
    /** Serial version uid. */
    private static final long serialVersionUID = 2251384437214194977L;

    /** {@inheritDoc} */
    @Override public double error(long sampleSize, double answer, double prediction) {
        return -(answer * Math.log(prediction) + (1 - answer) * Math.log(1 - prediction));
    }

    /** {@inheritDoc} */
    @Override public double gradient(long sampleSize, double answer, double prediction) {
        return (prediction - answer) / (prediction * (1.0 - prediction));
    }
}
