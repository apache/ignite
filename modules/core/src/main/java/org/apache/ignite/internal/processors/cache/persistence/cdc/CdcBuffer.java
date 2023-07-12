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

package org.apache.ignite.internal.processors.cache.persistence.cdc;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.Nullable;

/**
 * Buffer that stores WAL records before {@link CdcWorker} consumes it. Buffer is a single-producer single-consumer bounded queue.
 * <p>
 * TODO: Optimize the queue:
 *       1. by space using LinkedArrayQueue. Example: http://psy-lob-saw.blogspot.com/2016/10/linked-array-queues-part-1-spsc.html.
 *          It helps to avoid using AtomicLong for #size.
 *       2. by performance using AtomicReference#lazySet or similar for LinkedNode#next.
 */
public class CdcBuffer {
    /** Maximum size of the underlying buffer, bytes. */
    private final long maxSize;

    /** Reference to last consumed linked node. */
    private LinkedNode consumerNode;

    /** Reference to last produced linked node. */
    private volatile LinkedNode producerNode;

    /** Current size of the buffer, bytes. */
    private final AtomicLong size = new AtomicLong();

    /** If {@code true} then buffer has overflowed. */
    private volatile boolean overflowed;

    /** */
    public CdcBuffer(long maxSize) {
        assert maxSize > 0 : maxSize;

        this.maxSize = maxSize;

        producerNode = consumerNode = new LinkedNode(null);
    }

    /**
     * Offers data for the queue. Performs by the single producer thread.
     *
     * @param data Data to store in the buffer.
     */
    public boolean offer(ByteBuffer data) {
        int bufSize = data.limit() - data.position();

        if (size.addAndGet(bufSize) > maxSize) {
            overflowed = true;

            return false;
        }

        byte[] cp = new byte[bufSize];

        data.get(cp, 0, bufSize);

        LinkedNode newNode = new LinkedNode(ByteBuffer.wrap(cp));
        LinkedNode oldNode = producerNode;

        producerNode = newNode;
        oldNode.next(newNode);

        return true;
    }

    /**
     * Polls data from the queue. Performs by single consumer thread.
     *
     * @return Polled data, or {@code null} if no data is available now.
     */
    public ByteBuffer poll() {
        LinkedNode prev = consumerNode;

        LinkedNode next = prev.next;

        if (next != null)
            return poll(prev, next);
        else if (prev != producerNode) {
            while ((next = prev.next) == null) {
                // No-op. New reference should appear soon.
            }

            return poll(prev, next);
        }

        return null;
    }

    /** @return Current buffer size. */
    public long size() {
        return size.get();
    }

    /** Cleans the buffer if overflowed. Performs by the consumer thread. */
    public void cleanIfOverflowed() {
        if (!overflowed || consumerNode == null)
            return;

        ByteBuffer data;

        do {
            data = poll();
        }
        while (data != null);

        consumerNode = null;
        producerNode = null;

        size.set(0);
    }

    /**
     * @param prev Previously consumed linked node.
     * @param next Node to consume.
     * @return Data to consume.
     */
    private ByteBuffer poll(LinkedNode prev, LinkedNode next) {
        ByteBuffer data = next.data;

        prev.next = null;
        consumerNode = next;

        size.addAndGet(-(data.limit() - data.position()));

        return data;
    }

    /** */
    private static class LinkedNode {
        /** */
        private volatile @Nullable LinkedNode next;

        /** */
        private final ByteBuffer data;

        /** */
        LinkedNode(ByteBuffer data) {
            this.data = data;
        }

        /** */
        void next(LinkedNode next) {
            this.next = next;
        }
    }
}
