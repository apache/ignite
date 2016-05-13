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
 *
 */

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

/**
 * This class represents mutable cache state.
 */
public class CacheState implements Serializable {

    /** Active flag. */
    private final boolean active;

    private final Set<Integer> lostParts;

    /**
     * Constructor.
     * @param active Active flag.
     */
    public CacheState(boolean active) {
        this(active, null);
    }

    public CacheState(boolean active, Set<Integer> lostParts) {
        this.active = active;
        this.lostParts = lostParts != null ? Collections.unmodifiableSet(lostParts) : Collections.<Integer>emptySet();
    }

    /**
     * @return {@code True} if cache is active.
     */
    public boolean active() {
        return active;
    }

    public Set<Integer> lostPartitions() {
        return lostParts;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        CacheState state = (CacheState)o;

        if (active != state.active)
            return false;
        return lostParts != null ? lostParts.equals(state.lostParts) : state.lostParts == null;

    }

    @Override public int hashCode() {
        int result = (active ? 1 : 0);
        result = 31 * result + (lostParts != null ? lostParts.hashCode() : 0);
        return result;
    }
}
