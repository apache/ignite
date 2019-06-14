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

import java.io.Serializable;

/**
 * Loss interface of computing error or gradient of error on specific row in dataset.
 */
public interface Loss extends Serializable {
    /**
     * Error value for model answer.
     *
     * @param sampleSize Sample size.
     * @param lb Label.
     * @param mdlAnswer Model answer.
     * @return Error value.
     */
    public double error(long sampleSize, double lb, double mdlAnswer);

    /**
     * Error gradient value for model answer.
     *
     * @param sampleSize Sample size.
     * @param lb Label.
     * @param mdlAnswer Model answer.
     * @return Error value.
     */
    public double gradient(long sampleSize, double lb, double mdlAnswer);
}
