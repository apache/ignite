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

package org.apache.ignite.internal.processors.cache.query;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiTuple;

/** Represents cache key-value pair and score to compare cache entry by custom rule. */
public class ScoredCacheEntry<K, V> extends IgniteBiTuple<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private float score;

    /** */
    public ScoredCacheEntry() {}

    /** */
    public ScoredCacheEntry(K key, V val, float score) {
        super(key, val);

        this.score = score;
    }

    /** */
    public float score() {
        return score;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(score);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        score = (float)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ScoredCacheEntry.class, this);
    }
}

