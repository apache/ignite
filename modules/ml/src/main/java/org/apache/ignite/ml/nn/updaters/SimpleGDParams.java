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

/**
 * Parameters for {@link SimpleGDUpdater}.
 */
public class SimpleGDParams implements ModelUpdater<SmoothParametrized> {
    /**
     * Gradient.
     */
    private Vector gradient;

    /**
     * Learning rate.
     */
    private double learningRate;

    /**
     * Construct instance of this class.
     *
     * @param paramsCnt Count of parameters.
     * @param learningRate Learning rate.
     */
    public SimpleGDParams(int paramsCnt, double learningRate) {
        gradient = new DenseLocalOnHeapVector(paramsCnt);
        this.learningRate = learningRate;
    }

    /**
     * Construct instance of this class.
     *
     * @param gradient Gradient.
     * @param learningRate Learning rate.
     */
    public SimpleGDParams(Vector gradient, double learningRate) {
        this.gradient = gradient;
        this.learningRate = learningRate;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <M extends SmoothParametrized> M updateModel(M obj) {
        Vector params = obj.parameters();
        return (M)obj.setParameters(params.minus(gradient.times(learningRate)));
    }
}
