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

package org.apache.ignite.internal.cache.query.index.sorted.keys;

import java.util.UUID;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;

/** */
public class UuidIndexKey implements IndexKey {
    /** */
    private final UUID key;

    /** */
    public UuidIndexKey(UUID key) {
        this.key = key;
    }

    /** {@inheritDoc} */
    @Override public Object key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public IndexKeyType type() {
        return IndexKeyType.UUID;
    }

    /** {@inheritDoc} */
    @Override public int compare(IndexKey o) {
        UUID okey = (UUID)o.key();

        // Keep old logic.
        if (key.getMostSignificantBits() == okey.getMostSignificantBits())
            return Long.compare(key.getLeastSignificantBits(), okey.getLeastSignificantBits());
        else
            return key.getMostSignificantBits() > okey.getMostSignificantBits() ? 1 : -1;
    }
}
