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

package org.apache.ignite.loadtests.colocation;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;

/**
 * Accenture key.
 */
public class GridTestKey implements Externalizable {
    /** */
    private long id;

    /** */
    @AffinityKeyMapped
    private int affKey;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridTestKey() {
        // No-op.
    }

    /**
     * @param id ID.
     */
    GridTestKey(long id) {
        this.id = id;

        affKey = affinityKey(id);
    }

    /**
     * @return ID.
     */
    public long getId() {
        return id;
    }

    /**
     * @return Affinity key.
     */
    public int affinityKey() {
        return affKey;
    }

    /**
     * @param id ID.
     * @return Affinity key.
     */
    public static int affinityKey(long id) {
        return (int)(id % GridTestConstants.MOD_COUNT);
    }

    /**
     * Implement {@link Externalizable} for faster serialization. This is
     * optional and you can simply implement {@link Serializable}.
     *
     * @param in Input.
     * @throws IOException If failed.
     */
    @Override public void readExternal(ObjectInput in) throws IOException {
        id = in.readLong();
    }

    /**
     * Implement {@link Externalizable} for faster serialization. This is
     * optional and you can simply implement {@link Serializable}.
     *
     * @param out Output.
     * @throws IOException If failed.
     */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(id);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        GridTestKey key = (GridTestKey)o;

        return id == key.id;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return (int)(id ^ (id >>> 32));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "AccentureKey [id=" + id + ']';
    }
}
