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

package org.apache.ignite.marshaller.optimized;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.io.GridUnsafeDataInput;
import org.apache.ignite.internal.util.io.GridUnsafeDataOutput;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Storage for object streams.
 */
class OptimizedObjectStreamRegistry {
    /** Holders. */
    private static final ThreadLocal<StreamHolder> holders = new ThreadLocal<>();

    /** Output streams pool. */
    private static BlockingQueue<OptimizedObjectOutputStream> outPool;

    /** Input streams pool. */
    private static BlockingQueue<OptimizedObjectInputStream> inPool;

    /**
     * Ensures singleton.
     */
    private OptimizedObjectStreamRegistry() {
        // No-op.
    }

    /**
     * Sets streams pool size.
     *
     * @param size Streams pool size.
     */
    static void poolSize(int size) {
        if (size > 0) {
            outPool = new LinkedBlockingQueue<>(size);
            inPool = new LinkedBlockingQueue<>(size);

            for (int i = 0; i < size; i++) {
                outPool.offer(createOut());
                inPool.offer(createIn());
            }
        }
        else {
            outPool = null;
            inPool = null;
        }
    }

    /**
     * Gets output stream.
     *
     * @return Object output stream.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException If thread is interrupted while trying to take holder from pool.
     */
    static OptimizedObjectOutputStream out() throws IgniteInterruptedCheckedException {
        if (outPool != null) {
            try {
                return outPool.take();
            }
            catch (InterruptedException e) {
                throw new IgniteInterruptedCheckedException(
                    "Failed to take output object stream from pool (thread interrupted).", e);
            }
        }
        else
            return holder().acquireOut();
    }

    /**
     * Gets input stream.
     *
     * @return Object input stream.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException If thread is interrupted while trying to take holder from pool.
     */
    static OptimizedObjectInputStream in() throws IgniteInterruptedCheckedException {
        if (inPool != null) {
            try {
                return inPool.take();
            }
            catch (InterruptedException e) {
                throw new IgniteInterruptedCheckedException(
                    "Failed to take input object stream from pool (thread interrupted).", e);
            }
        }
        else
            return holder().acquireIn();
    }

    /**
     * Closes and releases output stream.
     *
     * @param out Object output stream.
     */
    static void closeOut(OptimizedObjectOutputStream out) {
        U.close(out, null);

        if (outPool != null) {
            boolean b = outPool.offer(out);

            assert b;
        }
        else {
            StreamHolder holder = holders.get();

            if (holder != null)
                holder.releaseOut();
        }
    }

    /**
     * Closes and releases input stream.
     *
     * @param in Object input stream.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    static void closeIn(OptimizedObjectInputStream in) {
        U.close(in, null);

        if (inPool != null) {
            boolean b = inPool.offer(in);

            assert b;
        }
        else {
            StreamHolder holder = holders.get();

            if (holder != null)
                holder.releaseIn();
        }
    }

    /**
     * Gets holder from pool or thread local.
     *
     * @return Stream holder.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException If thread is interrupted while trying to take holder from pool.
     */
    private static StreamHolder holder() throws IgniteInterruptedCheckedException {
        StreamHolder holder = holders.get();

        if (holder == null)
            holders.set(holder = new StreamHolder());

        return holder;
    }

    /**
     * Creates output stream.
     *
     * @return Object output stream.
     */
    private static OptimizedObjectOutputStream createOut() {
        try {
            return new OptimizedObjectOutputStream(new GridUnsafeDataOutput(4 * 1024));
        }
        catch (IOException e) {
            throw new IgniteException("Failed to create object output stream.", e);
        }
    }

    /**
     * Creates input stream.
     *
     * @return Object input stream.
     */
    private static OptimizedObjectInputStream createIn() {
        try {
            return new OptimizedObjectInputStream(new GridUnsafeDataInput());
        }
        catch (IOException e) {
            throw new IgniteException("Failed to create object input stream.", e);
        }
    }

    /**
     * Streams holder.
     */
    private static class StreamHolder {
        /** Output stream. */
        private final OptimizedObjectOutputStream out = createOut();

        /** Input stream. */
        private final OptimizedObjectInputStream in = createIn();

        /** Output streams counter. */
        private int outAcquireCnt;

        /** Input streams counter. */
        private int inAcquireCnt;

        /**
         * Gets output stream.
         *
         * @return Object output stream.
         */
        OptimizedObjectOutputStream acquireOut() {
            return outAcquireCnt++ > 0 ? createOut() : out;
        }

        /**
         * Gets input stream.
         *
         * @return Object input stream.
         */
        OptimizedObjectInputStream acquireIn() {
            return inAcquireCnt++ > 0 ? createIn() : in;
        }

        /**
         * Releases output stream.
         */
        void releaseOut() {
            outAcquireCnt--;
        }

        /**
         * Releases input stream.
         */
        void releaseIn() {
            inAcquireCnt--;
        }
    }
}
