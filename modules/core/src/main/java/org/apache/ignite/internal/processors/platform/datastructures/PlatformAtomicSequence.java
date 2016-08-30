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

package org.apache.ignite.internal.processors.platform.datastructures;

import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;

/**
 * Platform atomic sequence wrapper.
 */
public class PlatformAtomicSequence extends PlatformAbstractTarget {
    /** */
    private final IgniteAtomicSequence atomicSeq;

    /**
     * Ctor.
     * @param ctx Context.
     * @param atomicSeq AtomicSequence to wrap.
     */
    public PlatformAtomicSequence(PlatformContext ctx, IgniteAtomicSequence atomicSeq) {
        super(ctx);

        assert atomicSeq != null;

        this.atomicSeq = atomicSeq;
    }

    /**
     * Reads the value.
     *
     * @return Current atomic sequence value.
     */
    public long get() {
        return atomicSeq.get();
    }

    /**
     * Increments and reads the value.
     *
     * @return Current atomic sequence value.
     */
    public long incrementAndGet() {
        return atomicSeq.incrementAndGet();
    }

    /**
     * Reads and increments the value.
     *
     * @return Original atomic sequence value.
     */
    public long getAndIncrement() {
        return atomicSeq.getAndIncrement();
    }

    /**
     * Adds a value.
     *
     * @return Current atomic sequence value.
     */
    public long addAndGet(long l) {
        return atomicSeq.addAndGet(l);
    }

    /**
     * Adds a value.
     *
     * @return Original atomic sequence value.
     */
    public long getAndAdd(long l) {
        return atomicSeq.getAndAdd(l);
    }

    /**
     * Gets the batch size.
     *
     * @return Batch size.
     */
    public int getBatchSize() {
        return atomicSeq.batchSize();
    }

    /**
     * Sets the batch size.
     *
     * @param size Batch size.
     */
    public void setBatchSize(int size) {
        atomicSeq.batchSize(size);
    }

    /**
     * Gets status of atomic.
     *
     * @return {@code true} if atomic was removed from cache, {@code false} in other case.
     */
    public boolean isClosed() {
        return atomicSeq.removed();
    }

    /**
     * Removes this atomic.
     */
    public void close() {
        atomicSeq.close();
    }
}
