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

package org.apache.ignite.ml.nn.updaters;

import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.nn.MLP;

public class Gradients implements UpdaterParams {
    private Vector gradient;
    private double learningRate;

    public Gradients(int paramsCount, double learningRate) {
        gradient = new DenseLocalOnHeapVector(paramsCount);
        this.learningRate = learningRate;
    }

    public Gradients(Vector gradient, double learningRate) {
        this.gradient = gradient;
        this.learningRate = learningRate;
    }

    @Override public void updateMLP(MLP mlp) {
        Vector params = mlp.parameters();
        mlp.setParameters(params.plus(gradient.times(learningRate)));
    }
}
