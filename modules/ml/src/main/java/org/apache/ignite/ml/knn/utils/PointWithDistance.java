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


package org.apache.ignite.ml.knn.utils;

import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Utils class to be used in heap to compare two point using their distances to target point.
 * Note: this class has a natural ordering that is inconsistent with equals.
 *
 * @param <L> Label type.
 */
public final class PointWithDistance<L> implements Comparable<PointWithDistance> {
    /** Data point. */
    private final LabeledVector<L> pnt;

    /** Distance to target point. */
    private final double distance;

    /**
     * Constructs a new instance of data point with distance.
     *
     * @param pnt Data point.
     * @param distance Distance to target point.
     */
    public PointWithDistance(LabeledVector<L> pnt, double distance) {
        this.pnt = pnt;
        this.distance = distance;
    }

    /** */
    public LabeledVector<L> getPnt() {
        return pnt;
    }

    /** */
    public double getDistance() {
        return distance;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(PointWithDistance o) {
        return Double.compare(distance, o.distance);
    }
}
