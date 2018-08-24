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

package org.apache.ignite.ml.dataset.impl.bootstrapping;

import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Represents vector with repetitions counters for subsamples in bootstrapped dataset.
 */
public class BootstrappedVector {
    /** Features. */
    private final Vector features;
    /** Label. */
    private final double label;
    /** Repetitions counters. */
    private final int[] repetitionsCounters;

    /**
     * Creates an instance of BootstrappedVector.
     *
     * @param features Features.
     * @param label Label.
     * @param repetitionsCounters Repetitions counters.
     */
    public BootstrappedVector(Vector features, double label, int[] repetitionsCounters) {
        this.features = features;
        this.label = label;
        this.repetitionsCounters = repetitionsCounters;
    }

    /**
     * Returns features vector.
     *
     * @return Features.
     */
    public Vector getFeatures() {
        return features;
    }

    /**
     * Returns label for features vector.
     *
     * @return Label.
     */
    public double getLabel() {
        return label;
    }

    /**
     * Returns repetitions counters vector.
     *
     * @return repetitions counters vector.
     */
    public int[] getRepetitionsCounters() {
        return repetitionsCounters;
    }
}
