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

import org.apache.ignite.cache.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMemoryMode.*;
import static org.apache.ignite.cache.CacheMode.*;

/**
 * Configuration for Ignite collections.
 */
public class CollectionConfiguration implements Externalizable {
    /** Cache atomicity mode. */
    private CacheAtomicityMode atomicityMode = ATOMIC;

    /** Cache mode. */
    private CacheMode cacheMode = PARTITIONED;

    /** Cache memory mode. */
    private CacheMemoryMode memoryMode = ONHEAP_TIERED;

    /** Number of backups. */
    private int backups = 0;

    /** Off-heap memory size. */
    private long offHeapMaxMem = -1;

    /** Collocated flag. */
    private boolean collocated;

    /**
     * @return {@code True} if all items within the same collection will be collocated on the same node.
     */
    public boolean isCollocated() {
        return collocated;
    }

    /**
     * @param collocated If {@code true} then all items within the same collection will be collocated on the same node.
     *      Otherwise elements of the same set maybe be cached on different nodes. This parameter works only
     *      collections stored in {@link CacheMode#PARTITIONED} cache.
     */
    public void setCollocated(boolean collocated) {
        this.collocated = collocated;
    }

    /**
     * @return Cache atomicity mode.
     */
    public CacheAtomicityMode atomicityMode() {
        return atomicityMode;
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     */
    public void atomicityMode(CacheAtomicityMode atomicityMode) {
        this.atomicityMode = atomicityMode;
    }

    /**
     * @return Cache mode.
     */
    public CacheMode cacheMode() {
        return cacheMode;
    }

    /**
     * @param cacheMode Cache mode.
     */
    public void cacheMode(CacheMode cacheMode) {
        this.cacheMode = cacheMode;
    }

    /**
     * @return Cache memory mode.
     */
    public CacheMemoryMode memoryMode() {
        return memoryMode;
    }

    /**
     * @param memoryMode Memory mode.
     */
    public void memoryMode(CacheMemoryMode memoryMode) {
        this.memoryMode = memoryMode;
    }

    /**
     * @return Number of backups.
     */
    public int backups() {
        return backups;
    }

    /**
     * @param backups Cache number of backups.
     */
    public void backups(int backups) {
        this.backups = backups;
    }

    /**
     * @return Off-heap memory size.
     */
    public long offHeapMaxMem() {
        return offHeapMaxMem;
    }

    /**
     * @param offHeapMaxMem Off-heap memory size.
     */
    public void offHeapMaxMem(long offHeapMaxMem) {
        this.offHeapMaxMem = offHeapMaxMem;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CollectionConfiguration.class, this);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(atomicityMode.ordinal());
        out.writeInt(cacheMode.ordinal());
        out.writeObject(memoryMode);
        out.writeInt(backups);
        out.writeLong(offHeapMaxMem);
        out.writeBoolean(collocated);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        atomicityMode = CacheAtomicityMode.fromOrdinal(in.readInt());
        cacheMode = CacheMode.fromOrdinal(in.readInt());
        memoryMode = (CacheMemoryMode) in.readObject();
        backups = in.readInt();
        offHeapMaxMem = in.readLong();
        collocated = in.readBoolean();
    }
}
