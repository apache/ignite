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
package org.apache.ignite.ml.math.distances;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.ml.math.exceptions.math.CardinalityException;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Calculates {@code J = |A \cap B| / |A \cup B| } (Jaccard index) distance between two points.
 */
public class JaccardIndex implements DistanceMeasure {
    /** {@inheritDoc} */
    @Override public double compute(Vector a, Vector b) throws CardinalityException {
        Set<Double> uniqueValues = new HashSet<>();
        for (int i = 0; i < a.size(); i++)
            uniqueValues.add(a.get(i));

        double intersect = 0;
        for (int i = 0; i < b.size(); i++)
            if (uniqueValues.contains(b.get(i)))
                ++intersect;

        return intersect / (a.size() + b.size() - intersect);
    }
}
