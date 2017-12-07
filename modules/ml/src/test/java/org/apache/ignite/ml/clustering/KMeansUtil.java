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

package org.apache.ignite.ml.clustering;

import org.apache.ignite.ml.math.Vector;

import static org.junit.Assert.assertTrue;

/** Utilities for k-means tests. */
class KMeansUtil {
    /** */
    static void checkIsInEpsilonNeighbourhood(Vector[] v1s, Vector[] v2s, double epsilon) {
        for (int i = 0; i < v1s.length; i++) {
            assertTrue("Not in epsilon neighbourhood (index " + i + ") ",
                v1s[i].minus(v2s[i]).kNorm(2) < epsilon);
        }
    }
}
