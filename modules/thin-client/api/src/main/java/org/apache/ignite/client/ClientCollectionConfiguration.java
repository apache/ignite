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

package org.apache.ignite.client;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Configuration for Ignite collections.
 */
public class ClientCollectionConfiguration {
    /** Cache atomicity mode. */
    private CacheAtomicityMode atomicityMode = ATOMIC;

    /** Cache mode. */
    private CacheMode cacheMode = PARTITIONED;

    /** Number of backups. */
    private int backups;

    /** Colocated flag. */
    private boolean colocated;

    /** Group name. */
    private String grpName;

    /**
     * @return {@code True} if all items within the same collection will be collocated on the same node.
     */
    public boolean isColocated() {
        return colocated;
    }

    /**
     * @param colocated If {@code true} then all items within the same collection will be collocated on the same node.
     *      Otherwise elements of the same set maybe be cached on different nodes. This parameter works only
     *      collections stored in {@link CacheMode#PARTITIONED} cache.
     *
     * @return {@code this} for chaining.
     */
    public ClientCollectionConfiguration setColocated(boolean colocated) {
        this.colocated = colocated;

        return this;
    }

    /**
     * @return Cache atomicity mode.
     */
    public CacheAtomicityMode getAtomicityMode() {
        return atomicityMode;
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @return {@code this} for chaining.
     */
    public ClientCollectionConfiguration setAtomicityMode(CacheAtomicityMode atomicityMode) {
        this.atomicityMode = atomicityMode;

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
    public ClientCollectionConfiguration setCacheMode(CacheMode cacheMode) {
        this.cacheMode = cacheMode;

        return this;
    }

    /**
     * @return Number of backups.
     */
    public int getBackups() {
        return backups;
    }

    /**
     * @param backups Cache number of backups.
     * @return {@code this} for chaining.
     */
    public ClientCollectionConfiguration setBackups(int backups) {
        this.backups = backups;

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
    public ClientCollectionConfiguration setGroupName(String grpName) {
        this.grpName = grpName;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ClientCollectionConfiguration.class, this);
    }
}
