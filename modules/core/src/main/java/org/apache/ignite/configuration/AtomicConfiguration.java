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

package org.apache.ignite.configuration;

import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Configuration for atomic data structures.
 */
public class AtomicConfiguration {
    /** */
    public static final int DFLT_BACKUPS = 0;

    /** */
    public static final CacheMode DFLT_CACHE_MODE = PARTITIONED;

    /** Default atomic sequence reservation size. */
    public static final int DFLT_ATOMIC_SEQUENCE_RESERVE_SIZE = 1000;

    /** Default batch size for all cache's sequences. */
    private int seqReserveSize = DFLT_ATOMIC_SEQUENCE_RESERVE_SIZE;

    /** Cache mode. */
    private CacheMode cacheMode = DFLT_CACHE_MODE;

    /** Number of backups. */
    private int backups = DFLT_BACKUPS;

    /** Affinity function */
    private AffinityFunction aff;

    /** Group name. */
    private String grpName;

    /**
     * @return Number of backup nodes.
     */
    public int getBackups() {
        return backups;
    }

    /**
     * @param backups Number of backup nodes.
     * @return {@code this} for chaining.
     */
    public AtomicConfiguration setBackups(int backups) {
        this.backups = backups;

        return this;
    }

    /**
     * @return Cache mode.
     */
    public CacheMode getCacheMode() {
        return cacheMode;
    }

    /**
     * @param cacheMode Cache mode.
     * @return {@code this} for chaining.
     */
    public AtomicConfiguration setCacheMode(CacheMode cacheMode) {
        this.cacheMode = cacheMode;

        return this;
    }

    /**
     * Gets default number of sequence values reserved for {@link IgniteAtomicSequence} instances. After
     * a certain number has been reserved, consequent increments of sequence will happen locally,
     * without communication with other nodes, until the next reservation has to be made.
     * <p>
     * Default value is {@link #DFLT_ATOMIC_SEQUENCE_RESERVE_SIZE}.
     *
     * @return Atomic sequence reservation size.
     */
    public int getAtomicSequenceReserveSize() {
        return seqReserveSize;
    }

    /**
     * Sets default number of sequence values reserved for {@link IgniteAtomicSequence} instances. After a certain
     * number has been reserved, consequent increments of sequence will happen locally, without communication with other
     * nodes, until the next reservation has to be made.
     *
     * @param seqReserveSize Atomic sequence reservation size.
     * @see #getAtomicSequenceReserveSize()
     * @return {@code this} for chaining.
     */
    public AtomicConfiguration setAtomicSequenceReserveSize(int seqReserveSize) {
        this.seqReserveSize = seqReserveSize;

        return this;
    }

    /**
     * Gets atomic cache affinity function.
     *
     * @return Affinity function or null, if not set.
     */
    public AffinityFunction getAffinity() {
        return aff;
    }

    /**
     * Sets atomic cache affinity function.
     *
     * @param aff Affinity function.
     * @return {@code this} for chaining.
     */
    public AtomicConfiguration setAffinity(AffinityFunction aff) {
        this.aff = aff;

        return this;
    }

    /**
     * @return Group name.
     */
    public String getGroupName() {
        return grpName;
    }

    /**
     * @param grpName Group name.
     * @return {@code this} for chaining.
     */
    public AtomicConfiguration setGroupName(String grpName) {
        this.grpName = grpName;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(AtomicConfiguration.class, this);
    }
}