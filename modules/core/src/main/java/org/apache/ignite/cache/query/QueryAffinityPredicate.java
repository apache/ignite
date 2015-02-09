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

package org.apache.ignite.cache.query;

import org.apache.ignite.internal.util.typedef.internal.*;

import javax.cache.*;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public final class QueryAffinityPredicate<K, V> extends QueryPredicate<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Predicate. */
    private QueryPredicate<K, V> p;

    /** Keys. */
    private K[] keys;

    /** Partitions. */
    private int[] parts;

    /**
     * Empty constructor.
     */
    public QueryAffinityPredicate() {
        // No-op.
    }

    /**
     * Constructs affinity predicate with specified affinity keys.
     *
     * @param p Predicate.
     * @param keys Affinity keys.
     */
    public QueryAffinityPredicate(QueryPredicate<K, V> p, K... keys) {
        this.p = p;
        this.keys = keys;
    }

    /**
     * Constructs affinity predicate with specified affinity partitions.
     *
     * @param p Predicate.
     * @param parts Affinity partitions.
     */
    public QueryAffinityPredicate(QueryPredicate<K, V> p, int[] parts) {
        this.p = p;
        this.parts = parts;
    }

    /**
     * Gets wrapped predicate.
     *
     * @return Wrapped predicate.
     */
    public QueryPredicate<K, V> getPredicate() {
        return p;
    }

    /**
     * Sets wrapped predicate.
     *
     * @param p Wrapped predicate.
     */
    public void setPredicate(QueryPredicate<K, V> p) {
        this.p = p;
    }

    /**
     * Gets affinity keys.
     *
     * @return Affinity keys.
     */
    public K[] getKeys() {
        return keys;
    }

    /**
     * Sets affinity keys.
     *
     * @param keys Affinity keys.
     */
    public void setKeys(K... keys) {
        this.keys = keys;
    }

    /**
     * Gets affinity partitions.
     *
     * @return Affinity partitions.
     */
    public int[] getPartitions() {
        return parts;
    }

    /**
     * Sets affinity partitions.
     *
     * @param parts Affinity partitions.
     */
    public void setPartitions(int... parts) {
        this.parts = parts;
    }

    /** {@inheritDoc} */
    @Override public final boolean apply(Cache.Entry<K, V> entry) {
        return p.apply(entry);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryAffinityPredicate.class, this);
    }
}
