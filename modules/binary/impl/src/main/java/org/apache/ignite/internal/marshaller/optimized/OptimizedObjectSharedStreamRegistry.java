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

package org.apache.ignite.internal.marshaller.optimized;

import java.io.IOException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.CommonUtils;
import org.apache.ignite.internal.util.io.GridUnsafeDataInput;
import org.apache.ignite.internal.util.io.GridUnsafeDataOutput;

/**
 * Storage for object streams.
 */
class OptimizedObjectSharedStreamRegistry {
    /** */
    private static final ThreadLocal<StreamHolder> holders = new ThreadLocal<>();

    /**
     * Gets output stream.
     *
     * @return Object output stream.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException If thread is interrupted while trying to take holder from pool.
     */
    OptimizedObjectOutputStream out() {
        return holder().acquireOut();
    }

    /**
     * Gets input stream.
     *
     * @return Object input stream.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException If thread is interrupted while trying to take holder from pool.
     */
    OptimizedObjectInputStream in() {
        return holder().acquireIn();
    }

    /**
     * Closes and releases output stream.
     *
     * @param out Object output stream.
     */
    void closeOut(OptimizedObjectOutputStream out) {
        CommonUtils.close(out, null);

        StreamHolder holder = holders.get();

        if (holder != null)
            holder.releaseOut();
    }

    /**
     * Closes and releases input stream.
     *
     * @param in Object input stream.
     */
    void closeIn(OptimizedObjectInputStream in) {
        CommonUtils.close(in, null);

        StreamHolder holder = holders.get();

        if (holder != null)
            holder.releaseIn();
    }

    /**
     * Closes and releases not cached input stream.
     *
     * @param in Object input stream.
     */
    void closeNotCachedIn(OptimizedObjectInputStream in) {
        CommonUtils.close(in, null);

        StreamHolder holder = holders.get();

        if (holder != null) {
            holder.releaseIn();

            holders.set(null);
        }
    }

    /**
     * Creates output stream.
     *
     * @return Object output stream.
     */
    static OptimizedObjectOutputStream createOut() {
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
    static OptimizedObjectInputStream createIn() {
        try {
            return new OptimizedObjectInputStream(new GridUnsafeDataInput());
        }
        catch (IOException e) {
            throw new IgniteException("Failed to create object input stream.", e);
        }
    }

    /**
     * Gets holder from pool or thread local.
     *
     * @return Stream holder.
     */
    private static StreamHolder holder() {
        StreamHolder holder = holders.get();

        if (holder == null)
            holders.set(holder = new StreamHolder());

        return holder;
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
