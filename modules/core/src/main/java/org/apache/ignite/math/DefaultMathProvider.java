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

package org.apache.ignite.math;

import org.apache.ignite.cluster.*;
import org.apache.ignite.math.impls.*;
import java.io.*;
import java.util.*;

/**
 * Default built-in math provider.
 */
public class DefaultMathProvider implements MathProvider {
    /**
     * Creates default math provider.
     */
    public DefaultMathProvider() {
        // No-op.
    }

    @Override
    public Optional<Matrix> matrix(String flavor, Map<String, Object> args, ClusterGroup grp) {
        String flavorNorm = flavor.trim().toLowerCase();

        switch (flavorNorm) {
            case "dense.local.onheap":
                if (grp != null)
                    throw new UnsupportedOperationException(
                        String.format("Matrix flavor '%s' does not support clustering.", flavorNorm)
                    );

                // TODO: ignoring arguments for now.
                return Optional.of(new DenseLocalOnHeapMatrix());

            default:
                return Optional.empty();
        }
    }

    @Override
    public Optional<Vector> vector(String flavor, Map<String, Object> args, ClusterGroup grp) {
        String flavorNorm = flavor.trim().toLowerCase();

        switch (flavorNorm) {
            case "dense.local.onheap":
                if (grp != null)
                    throw new UnsupportedOperationException(
                        String.format("Vector flavor '%s' does not support clustering.", flavorNorm)
                    );

                // TODO: ignoring arguments for now.
                return Optional.of(new DenseLocalOnHeapVector());

            default:
                return Optional.empty();
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        // No-op.
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // No-op.
    }
}
