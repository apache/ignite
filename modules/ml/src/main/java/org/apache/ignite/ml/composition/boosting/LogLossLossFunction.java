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

public class LogLossLossFunction implements LossFunction {
    @Override public Double estimate(Double answer, Double prediction) {
        prediction = clipPrediction(prediction);
        return -(answer * Math.log(prediction) + (1. - answer) * Math.log(1. - prediction));
    }

    @Override public Double grad(Long sampleSize, Double answer, Double prediction) {
        return (prediction - answer) / (prediction * (1.0 - prediction));
    }

    private Double clipPrediction(Double prediction) {
        if (prediction > 0.99)
            return 0.99;
        else if (prediction < 0.01)
            return 0.01;
        else
            return prediction;
    }
}
