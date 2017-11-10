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

package org.apache.ignite.ml.trees;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Information about region used by continuous features.
 */
public class ContinuousRegionInfo extends RegionInfo {
    /**
     * Count of samples in this region.
     */
    private int size;

    /**
     * @param impurity Impurity of the region.
     * @param size Size of this region
     */
    public ContinuousRegionInfo(double impurity, int size) {
        super(impurity);
        this.size = size;
    }

    /**
     * No-op constructor for serialization/deserialization.
     */
    public ContinuousRegionInfo() {
        // No-op
    }

    /**
     * Get the size of region.
     */
    public int getSize() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "ContinuousRegionInfo [" +
            "size=" + size +
            ']';
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(size);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        size = in.readInt();
    }
}