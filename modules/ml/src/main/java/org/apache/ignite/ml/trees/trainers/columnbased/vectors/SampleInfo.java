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

package org.apache.ignite.ml.trees.trainers.columnbased.vectors;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Information about given sample within given fixed feature.
 */
public class SampleInfo implements Externalizable {
    /** Value of projection of this sample on given fixed feature. */
    private double val;

    /** Sample index. */
    private int sampleIdx;

    /**
     * @param val Value of projection of this sample on given fixed feature.
     * @param sampleIdx Sample index.
     */
    public SampleInfo(double val, int sampleIdx) {
        this.val = val;
        this.sampleIdx = sampleIdx;
    }

    /**
     * No-op constructor used for serialization/deserialization.
     */
    public SampleInfo() {
        // No-op.
    }

    /**
     * Get the value of projection of this sample on given fixed feature.
     *
     * @return Value of projection of this sample on given fixed feature.
     */
    public double val() {
        return val;
    }

    /**
     * Get the sample index.
     *
     * @return Sample index.
     */
    public int sampleInd() {
        return sampleIdx;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeDouble(val);
        out.writeInt(sampleIdx);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        val = in.readDouble();
        sampleIdx = in.readInt();
    }
}
