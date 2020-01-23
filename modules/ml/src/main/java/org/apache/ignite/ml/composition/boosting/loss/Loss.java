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
