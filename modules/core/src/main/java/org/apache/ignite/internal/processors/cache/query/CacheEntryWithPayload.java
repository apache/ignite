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
import org.apache.ignite.lang.IgniteBiTuple;

/** Represents cache key-value pair and additional payload to compare cache entity by custom rule. */
public class CacheEntryWithPayload<K, V, P> extends IgniteBiTuple<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private P payload;

    /** */
    public CacheEntryWithPayload() {}

    /** */
    public CacheEntryWithPayload(K key, V val, P payload) {
        super(key, val);

        this.payload = payload;
    }

    /** */
    public Object payload() {
        return payload;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(payload);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        payload = (P)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "CacheEntryWithPayload{" +
            "entry=" + super.toString() +
            ", payload=" + payload +
            '}';
    }
}

