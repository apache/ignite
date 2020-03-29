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

package org.apache.ignite.internal.processors.datastructures;

import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.IgniteSet;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public enum DataStructureType {
    /** */
    ATOMIC_LONG(IgniteAtomicLong.class.getSimpleName()),

    /** */
    ATOMIC_REF(IgniteAtomicReference.class.getSimpleName()),

    /** */
    ATOMIC_SEQ(IgniteAtomicSequence.class.getSimpleName()),

    /** */
    ATOMIC_STAMPED(IgniteAtomicStamped.class.getSimpleName()),

    /** */
    COUNT_DOWN_LATCH(IgniteCountDownLatch.class.getSimpleName()),

    /** */
    QUEUE(IgniteQueue.class.getSimpleName()),

    /** */
    SET(IgniteSet.class.getSimpleName()),

    /** */
    SEMAPHORE(IgniteSemaphore.class.getSimpleName()),

    /** */
    REENTRANT_LOCK(IgniteLock.class.getSimpleName());

    /** */
    private static final DataStructureType[] VALS = values();

    /** */
    private String name;

    /**
     * @param name Name.
     */
    DataStructureType(String name) {
        this.name = name;
    }

    /**
     * @return Data structure public class name.
     */
    public String className() {
        return name;
    }

    /**
     * @return {@code True} if this data structure type is volatile.
     */
    public boolean isVolatile() {
        return this == REENTRANT_LOCK || this == SEMAPHORE || this == COUNT_DOWN_LATCH;
    }

    /**
     * @return {@code True} if this data structure type is collection.
     */
    public boolean isCollection() {
        return this == SET || this == QUEUE;
    }

    /**
     * @param ord Ordinal value.
     * @return Enumerated value or {@code null} if ordinal out of range.
     */
    @Nullable public static DataStructureType fromOrdinal(int ord) {
        return ord >= 0 && ord < VALS.length ? VALS[ord] : null;
    }
}

