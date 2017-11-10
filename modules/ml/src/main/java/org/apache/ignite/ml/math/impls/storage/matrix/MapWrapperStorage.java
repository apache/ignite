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

package org.apache.ignite.ml.math.impls.storage.matrix;

import org.apache.ignite.internal.util.GridArgumentCheck;
import org.apache.ignite.ml.math.VectorStorage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Set;

/**
 * Storage for wrapping given map.
 */
public class MapWrapperStorage implements VectorStorage {
    /** Underlying map. */
    private Map<Integer, Double> data;

    /** Vector size. */
    private int size;

    /**
     * Construct a wrapper around given map.
     *
     * @param map Map to wrap.
     */
    public MapWrapperStorage(Map<Integer, Double> map) {
        data = map;

        Set<Integer> keys = map.keySet();

        GridArgumentCheck.notEmpty(keys, "map");

        Integer min = keys.stream().mapToInt(Integer::valueOf).min().getAsInt();
        Integer max = keys.stream().mapToInt(Integer::valueOf).max().getAsInt();

        assert min >= 0;

        size =  (max - min) + 1;
    }

    /**
     * No-op constructor for serialization.
     */
    public MapWrapperStorage() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public double get(int i) {
        return data.getOrDefault(i, 0.0);
    }

    /** {@inheritDoc} */
    @Override public void set(int i, double v) {
        if (v != 0.0)
            data.put(i, v);
        else if (data.containsKey(i))
            data.remove(i);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(data);
        out.writeInt(size);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        data = (Map<Integer, Double>)in.readObject();
        size = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isRandomAccess() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDistributed() {
        return false;
    }
}
