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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.BitSet;

/**
 * Information about categorical region.
 */
public class CategoricalRegionInfo extends RegionInfo implements Externalizable {
    /**
     * Bitset representing categories of this region.
     */
    private BitSet cats;

    /**
     * @param impurity Impurity of region.
     * @param cats Bitset representing categories of this region.
     */
    public CategoricalRegionInfo(double impurity, BitSet cats) {
        super(impurity);

        this.cats = cats;
    }

    /**
     * No-op constructor for serialization/deserialization.
     */
    public CategoricalRegionInfo() {
        // No-op
    }

    /**
     * Get bitset representing categories of this region.
     *
     * @return Bitset representing categories of this region.
     */
    public BitSet cats() {
        return cats;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(cats);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        cats = (BitSet)in.readObject();
    }
}
