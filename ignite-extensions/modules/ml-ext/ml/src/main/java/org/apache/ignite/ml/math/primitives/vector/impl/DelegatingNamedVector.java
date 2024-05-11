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

package org.apache.ignite.ml.math.primitives.vector.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.ml.math.primitives.vector.NamedVector;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Delegating named vector that delegates all operations to underlying vector and adds implementation of
 * {@link NamedVector} functionality using embedded map that maps string index on real integer index.
 */
public class DelegatingNamedVector extends DelegatingVector implements NamedVector {
    /** */
    private static final long serialVersionUID = -3425468245964928754L;

    /** Map that maps string index on real integer index. */
    private Map<String, Integer> map;

    /**
     * Constructs a new instance of delegating named vector.
     */
    public DelegatingNamedVector() {
        this.map = Collections.emptyMap();
    }

    /**
     * Constructs a new instance of delegating named vector.
     *
     * @param vector Underlying vector.
     * @param map Map that maps string index on real integer index.
     */
    public DelegatingNamedVector(Vector vector, Map<String, Integer> map) {
        super(vector);

        this.map = Objects.requireNonNull(map);
    }

    /** {@inheritDoc} */
    @Override public double get(String idx) {
        int intIdx = Objects.requireNonNull(map.get(idx), "Index not found [name='" + idx + "']");

        return get(intIdx);
    }

    /** {@inheritDoc} */
    @Override public NamedVector set(String idx, double val) {
        int intIdx = Objects.requireNonNull(map.get(idx), "Index not found [name='" + idx + "']");

        set(intIdx, val);

        return this;
    }

    /** {@inheritDoc} */
    @Override public Set<String> getKeys() {
        return map.keySet();
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(map);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        map = (Map<String, Integer>)in.readObject();
    }
}
