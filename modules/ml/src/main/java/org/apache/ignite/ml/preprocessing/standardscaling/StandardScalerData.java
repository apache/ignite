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
package org.apache.ignite.ml.preprocessing.standardscaling;

/** A Service class for {@link StandardScalerTrainer} which used for sums holing. */
public class StandardScalerData implements AutoCloseable {
    /** Sum values of every feature. */
    double[] sum;

    /** Sum of squared values of every feature. */
    double[] squaredSum;

    /** Rows count */
    long cnt;

    /**
     * Creates {@code StandardScalerData}.
     *
     * @param sum Sum values of every feature.
     * @param squaredSum Sum of squared values of every feature.
     * @param cnt Rows count.
     */
    public StandardScalerData(double[] sum, double[] squaredSum, long cnt) {
        this.sum = sum;
        this.squaredSum = squaredSum;
        this.cnt = cnt;
    }

    /** Merges to current. */
    StandardScalerData merge(StandardScalerData that) {
        for (int i = 0; i < sum.length; i++) {
            sum[i] += that.sum[i];
            squaredSum[i] += that.squaredSum[i];
        }

        cnt += that.cnt;
        return this;
    }

    /** */
    @Override public void close() {
        // Do nothing, GC will clean up.
    }
}
