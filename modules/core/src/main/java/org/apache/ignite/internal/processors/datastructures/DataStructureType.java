/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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

