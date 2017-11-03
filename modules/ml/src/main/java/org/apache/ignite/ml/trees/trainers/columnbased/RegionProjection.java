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

package org.apache.ignite.ml.trees.trainers.columnbased;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.ml.trees.RegionInfo;

/**
 * Projection of region on given feature.
 *
 * @param <D> Data of region.
 */
public class RegionProjection<D extends RegionInfo> implements Externalizable {
    /** Samples projections. */
    protected Integer[] sampleIndexes;

    /** Region data */
    protected D data;

    /** Depth of this region. */
    protected int depth;

    /**
     * @param sampleIndexes Samples indexes.
     * @param data Region data.
     * @param depth Depth of this region.
     */
    public RegionProjection(Integer[] sampleIndexes, D data, int depth) {
        this.data = data;
        this.depth = depth;
        this.sampleIndexes = sampleIndexes;
    }

    /**
     * No-op constructor used for serialization/deserialization.
     */
    public RegionProjection() {
        // No-op.
    }

    /**
     * Get samples indexes.
     *
     * @return Samples indexes.
     */
    public Integer[] sampleIndexes() {
        return sampleIndexes;
    }

    /**
     * Get region data.
     *
     * @return Region data.
     */
    public D data() {
        return data;
    }

    /**
     * Get region depth.
     *
     * @return Region depth.
     */
    public int depth() {
        return depth;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(sampleIndexes.length);

        for (Integer sampleIndex : sampleIndexes)
            out.writeInt(sampleIndex);

        out.writeObject(data);
        out.writeInt(depth);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int size = in.readInt();

        sampleIndexes = new Integer[size];

        for (int i = 0; i < size; i++)
            sampleIndexes[i] = in.readInt();

        data = (D)in.readObject();
        depth = in.readInt();
    }
}
