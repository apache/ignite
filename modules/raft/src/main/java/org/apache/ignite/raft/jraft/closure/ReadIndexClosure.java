/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.closure;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read index closure
 */
public abstract class ReadIndexClosure implements Closure {
    private static final Logger LOG = LoggerFactory.getLogger(ReadIndexClosure.class);

    private static final AtomicIntegerFieldUpdater<ReadIndexClosure> STATE_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(ReadIndexClosure.class, "state");

    private static final int PENDING = 0;
    private static final int COMPLETE = 1;

    /**
     * Invalid log index -1.
     */
    public static final long INVALID_LOG_INDEX = -1;

    private long index = INVALID_LOG_INDEX;
    private byte[] requestContext;

    private volatile int state = PENDING; // NOPMD

    /**
     * Called when ReadIndex can be executed.
     *
     * @param status the readIndex status.
     * @param index the committed index when starts readIndex.
     * @param reqCtx the request context passed by {@link Node#readIndex(byte[], ReadIndexClosure)}.
     * @see Node#readIndex(byte[], ReadIndexClosure)
     */
    public abstract void run(final Status status, final long index, final byte[] reqCtx);

    /**
     * Set callback result, called by jraft.
     *
     * @param index the committed index.
     * @param reqCtx the request context passed by {@link Node#readIndex(byte[], ReadIndexClosure)}.
     */
    public void setResult(final long index, final byte[] reqCtx) {
        this.index = index;
        this.requestContext = reqCtx;
    }

    /**
     * The committed log index when starts readIndex request. return -1 if fails.
     *
     * @return returns the committed index.  returns -1 if fails.
     */
    public long getIndex() {
        return this.index;
    }

    /**
     * Returns the request context.
     *
     * @return the request context.
     */
    public byte[] getRequestContext() {
        return this.requestContext;
    }

    @Override
    public void run(final Status status) {
        if (!STATE_UPDATER.compareAndSet(this, PENDING, COMPLETE)) {
            LOG.warn("A timeout read-index response finally returned: {}.", status);
            return;
        }

        try {
            run(status, this.index, this.requestContext);
        }
        catch (final Throwable t) {
            LOG.error("Fail to run ReadIndexClosure with status: {}.", status, t);
        }
    }
}
