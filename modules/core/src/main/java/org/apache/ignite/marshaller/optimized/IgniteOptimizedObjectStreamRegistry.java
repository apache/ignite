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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.util.io.*;

import java.io.*;
import java.util.concurrent.*;

/**
 * Storage for object streams.
 */
class IgniteOptimizedObjectStreamRegistry {
    /** Holders. */
    private static final ThreadLocal<StreamHolder> holders = new ThreadLocal<>();

    /** Holders pool. */
    private static BlockingQueue<StreamHolder> pool;

    /**
     * Ensures singleton.
     */
    private IgniteOptimizedObjectStreamRegistry() {
        // No-op.
    }

    /**
     * Sets streams pool size.
     *
     * @param size Streams pool size.
     */
    static void poolSize(int size) {
        if (size > 0) {
            pool = new LinkedBlockingQueue<>(size);

            for (int i = 0; i < size; i++) {
                boolean b = pool.offer(new StreamHolder());

                assert b;
            }
        }
        else
            pool = null;
    }

    /**
     * Gets output stream.
     *
     * @return Object output stream.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException If thread is interrupted while trying to take holder from pool.
     */
    static IgniteOptimizedObjectOutputStream out() throws IgniteInterruptedCheckedException {
        return holder().acquireOut();
    }

    /**
     * Gets input stream.
     *
     * @return Object input stream.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException If thread is interrupted while trying to take holder from pool.
     */
    static IgniteOptimizedObjectInputStream in() throws IgniteInterruptedCheckedException {
        return holder().acquireIn();
    }

    /**
     * Closes and releases output stream.
     *
     * @param out Object output stream.
     */
    static void closeOut(IgniteOptimizedObjectOutputStream out) {
        U.close(out, null);

        StreamHolder holder = holders.get();

        holder.releaseOut();

        if (pool != null) {
            holders.remove();

            boolean b = pool.offer(holder);

            assert b;
        }
    }

    /**
     * Closes and releases input stream.
     *
     * @param in Object input stream.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    static void closeIn(IgniteOptimizedObjectInputStream in) {
        U.close(in, null);

        StreamHolder holder = holders.get();

        holder.releaseIn();

        if (pool != null) {
            holders.remove();

            boolean b = pool.offer(holder);

            assert b;
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

        if (holder == null) {
            try {
                holders.set(holder = pool != null ? pool.take() : new StreamHolder());
            }
            catch (InterruptedException e) {
                throw new IgniteInterruptedCheckedException("Failed to take object stream from pool (thread interrupted).", e);
            }
        }

        return holder;
    }

    /**
     * Streams holder.
     */
    private static class StreamHolder {
        /** Output stream. */
        private final IgniteOptimizedObjectOutputStream out = createOut();

        /** Input stream. */
        private final IgniteOptimizedObjectInputStream in = createIn();

        /** Output streams counter. */
        private int outAcquireCnt;

        /** Input streams counter. */
        private int inAcquireCnt;

        /**
         * Gets output stream.
         *
         * @return Object output stream.
         */
        IgniteOptimizedObjectOutputStream acquireOut() {
            return outAcquireCnt++ > 0 ? createOut() : out;
        }

        /**
         * Gets input stream.
         *
         * @return Object input stream.
         */
        IgniteOptimizedObjectInputStream acquireIn() {
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

        /**
         * Creates output stream.
         *
         * @return Object output stream.
         */
        private IgniteOptimizedObjectOutputStream createOut() {
            try {
                return new IgniteOptimizedObjectOutputStream(new GridUnsafeDataOutput(4 * 1024));
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
        private IgniteOptimizedObjectInputStream createIn() {
            try {
                return new IgniteOptimizedObjectInputStream(new GridUnsafeDataInput());
            }
            catch (IOException e) {
                throw new IgniteException("Failed to create object input stream.", e);
            }
        }
    }
}
