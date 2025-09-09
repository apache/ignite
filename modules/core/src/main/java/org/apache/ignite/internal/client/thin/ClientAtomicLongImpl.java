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

package org.apache.ignite.internal.client.thin;

import org.apache.ignite.IgniteException;
import org.apache.ignite.client.ClientAtomicLong;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.BinaryWriterEx;
import org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor;
import org.jetbrains.annotations.Nullable;

/**
 * Client atomic long.
 */
public class ClientAtomicLongImpl implements ClientAtomicLong {
    /** */
    private final String name;

    /** */
    private final String groupName;

    /** */
    private final ReliableChannel ch;

    /** Cache id. */
    private final int cacheId;

    /**
     * Constructor.
     *
     * @param name Atomic long name.
     * @param groupName Cache group name.
     * @param ch Channel.
     */
    public ClientAtomicLongImpl(String name, @Nullable String groupName, ReliableChannel ch) {
        // name and groupName uniquely identify the data structure.
        this.name = name;
        this.groupName = groupName;
        this.ch = ch;

        String grpNameInternal = groupName == null ? DataStructuresProcessor.DEFAULT_DS_GROUP_NAME : groupName;
        cacheId = ClientUtils.cacheId(DataStructuresProcessor.ATOMICS_CACHE_NAME + "@" + grpNameInternal);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public long get() throws IgniteException {
        return ch.affinityService(cacheId, affinityKey(), ClientOperation.ATOMIC_LONG_VALUE_GET, this::writeName, in -> in.in().readLong());
    }

    /** {@inheritDoc} */
    @Override public long incrementAndGet() throws IgniteException {
        return addAndGet(1);
    }

    /** {@inheritDoc} */
    @Override public long getAndIncrement() throws IgniteException {
        return incrementAndGet() - 1;
    }

    /** {@inheritDoc} */
    @Override public long addAndGet(long l) throws IgniteException {
        return ch.affinityService(cacheId, affinityKey(), ClientOperation.ATOMIC_LONG_VALUE_ADD_AND_GET, out -> {
            writeName(out);
            out.out().writeLong(l);
        }, in -> in.in().readLong());
    }

    /** {@inheritDoc} */
    @Override public long getAndAdd(long l) throws IgniteException {
        return addAndGet(l) - l;
    }

    /** {@inheritDoc} */
    @Override public long decrementAndGet() throws IgniteException {
        return addAndGet(-1);
    }

    /** {@inheritDoc} */
    @Override public long getAndDecrement() throws IgniteException {
        return decrementAndGet() + 1;
    }

    /** {@inheritDoc} */
    @Override public long getAndSet(long l) throws IgniteException {
        return ch.affinityService(cacheId, affinityKey(), ClientOperation.ATOMIC_LONG_VALUE_GET_AND_SET, out -> {
            writeName(out);
            out.out().writeLong(l);
        }, in -> in.in().readLong());
    }

    /** {@inheritDoc} */
    @Override public boolean compareAndSet(long expVal, long newVal) throws IgniteException {
        return ch.affinityService(cacheId, affinityKey(), ClientOperation.ATOMIC_LONG_VALUE_COMPARE_AND_SET, out -> {
            writeName(out);
            out.out().writeLong(expVal);
            out.out().writeLong(newVal);
        }, in -> in.in().readBoolean());
    }

    /** {@inheritDoc} */
    @Override public boolean removed() {
        return ch.affinityService(cacheId, affinityKey(), ClientOperation.ATOMIC_LONG_EXISTS, this::writeName,
                in -> !in.in().readBoolean());
    }

    /** {@inheritDoc} */
    @Override public void close() {
        ch.affinityService(cacheId, affinityKey(), ClientOperation.ATOMIC_LONG_REMOVE, this::writeName, null);
    }

    /**
     * Writes the name.
     *
     * @param out Output channel.
     */
    private void writeName(PayloadOutputChannel out) {
        try (BinaryWriterEx w = BinaryUtils.writer(null, out.out(), null)) {
            w.writeString(name);
            w.writeString(groupName);
        }
    }

    /**
     * Gets the affinity key for this data structure.
     * 
     * @return Affinity key.
     */
    private String affinityKey() {
        // GridCacheInternalKeyImpl uses name as AffinityKeyMapped.
        return name;
    }
}
