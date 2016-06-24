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
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * This class represents mutable cache state.
 */
public class CacheState implements Serializable {
    /** Active flag. */
    private final boolean active;

    /** Lost partitions. */
    private final Set<Integer> lostParts;

    /**
     * Constructor.
     * @param active Active flag.
     */
    public CacheState(boolean active) {
        this(active, null);
    }

    /**
     * Constructor.
     * @param active Active flag.
     * @param lostParts Lost partitions.
     */
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

    /**
     * @return Lost partitions.
     */
    public Set<Integer> lostPartitions() {
        return lostParts;
    }

    /**
     * @param diff Diff to apply.
     * @return New CacheState with diff applied.
     */
    public CacheState apply(Difference diff) {
        boolean active = diff.active != null ? diff.active : this.active;

        Set<Integer> lostParts = new HashSet<>(this.lostParts.size());

        lostParts.addAll(this.lostParts);

        if (diff.recoveredLostParts() != null)
            lostParts.removeAll(diff.recoveredLostParts);

        if (diff.addedLostParts() != null)
            lostParts.addAll(diff.addedLostParts);

        return new CacheState(active, lostParts);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheState.class, this);
    }

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = (active ? 1 : 0);
        result = 31 * result + (lostParts != null ? lostParts.hashCode() : 0);
        return result;
    }

    /**
     * Modification of cache state.
     */
    public static class Difference implements Serializable {

        /** Active flag. */
        @Nullable private Boolean active;

        /** Added lost partitions. */
        @Nullable private Set<Integer> addedLostParts;

        /** Recovered lost partitions. */
        @Nullable private Set<Integer> recoveredLostParts;

        /**
         * @param active Active flag.
         * @param addedLostParts Added lost partitions.
         * @param recoveredLostParts Recovered lost partitions.
         */
        public Difference(@Nullable Boolean active, @Nullable Set<Integer> addedLostParts,
            @Nullable Set<Integer> recoveredLostParts) {
            this.active = active;

            if (addedLostParts != null)
                this.addedLostParts = new HashSet<>(addedLostParts);

            if (recoveredLostParts != null)
                this.recoveredLostParts = new HashSet<>(recoveredLostParts);
        }

        /**
         * @return Active flag.
         */
        @Nullable public Boolean active() {
            return active;
        }

        /**
         * @param active Active flag.
         */
        public void active(@Nullable Boolean active) {
            this.active = active;
        }

        /**
         * @return Added lost partitions.
         */
        @Nullable public Set<Integer> addedLostParts() {
            return addedLostParts;
        }

        /**
         * @param addedLostParts Added lost partitions.
         */
        public void addedLostParts(@Nullable Set<Integer> addedLostParts) {
            this.addedLostParts = addedLostParts;
        }

        /**
         * @return Recovered lost partitions.
         */
        @Nullable public Set<Integer> recoveredLostParts() {
            return recoveredLostParts;
        }

        /**
         * @param recoveredLostParts Recovered lost partitions.
         */
        public void recoveredLostParts(@Nullable Set<Integer> recoveredLostParts) {
            this.recoveredLostParts = recoveredLostParts;
        }

        public void merge(Difference that) {
            if (that.active != null)
                this.active = that.active;

            if (that.recoveredLostParts != null) {
                if (this.recoveredLostParts == null)
                    this.recoveredLostParts = that.recoveredLostParts;
                else
                    this.recoveredLostParts.addAll(that.recoveredLostParts);

                if (this.addedLostParts != null)
                    this.addedLostParts.removeAll(that.recoveredLostParts);
            }

            if (that.addedLostParts != null) {
                if (this.addedLostParts == null)
                    this.addedLostParts = that.addedLostParts;
                else
                    this.addedLostParts.addAll(that.addedLostParts);

                if (this.recoveredLostParts != null)
                    this.recoveredLostParts.removeAll(that.addedLostParts);
            }
        }

        @Override public String toString() {
            return S.toString(Difference.class, this);
        }

        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Difference that = (Difference)o;

            if (active != null ? !active.equals(that.active) : that.active != null)
                return false;
            if (addedLostParts != null ? !addedLostParts.equals(that.addedLostParts) : that.addedLostParts != null)
                return false;
            return recoveredLostParts != null ? recoveredLostParts.equals(that.recoveredLostParts) : that.recoveredLostParts == null;

        }

        @Override public int hashCode() {
            int result = active != null ? active.hashCode() : 0;
            result = 31 * result + (addedLostParts != null ? addedLostParts.hashCode() : 0);
            result = 31 * result + (recoveredLostParts != null ? recoveredLostParts.hashCode() : 0);
            return result;
        }
    }
}
