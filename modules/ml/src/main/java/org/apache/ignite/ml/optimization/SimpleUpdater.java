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

package org.apache.ignite.ml.optimization;

import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Simple updater with fixed learning rate which doesn't guarantee convergence.
 */
public class SimpleUpdater implements Updater {
    /** */
    private static final long serialVersionUID = 6417716224818162225L;

    /** */
    private final double learningRate;

    /** */
    public SimpleUpdater(double learningRate) {
        assert learningRate > 0;

        this.learningRate = learningRate;
    }

    /**
     * {@inheritDoc}
     */
    @Override public Vector compute(Vector oldWeights, Vector oldGradient, Vector weights, Vector gradient,
        int iteration) {
        return weights.minus(gradient.times(learningRate));
    }
}
