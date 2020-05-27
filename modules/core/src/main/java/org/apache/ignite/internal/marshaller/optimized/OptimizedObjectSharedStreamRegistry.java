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

import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class OptimizedObjectSharedStreamRegistry extends OptimizedObjectStreamRegistry {
    /** */
    private static final ThreadLocal<StreamHolder> holders = new ThreadLocal<>();

    /** {@inheritDoc} */
    @Override OptimizedObjectOutputStream out() {
        return holder().acquireOut();
    }

    /** {@inheritDoc} */
    @Override OptimizedObjectInputStream in() {
        return holder().acquireIn();
    }

    /** {@inheritDoc} */
    @Override void closeOut(OptimizedObjectOutputStream out) {
        U.close(out, null);

        StreamHolder holder = holders.get();

        if (holder != null)
            holder.releaseOut();
    }

    /** {@inheritDoc} */
    @Override void closeIn(OptimizedObjectInputStream in) {
        U.close(in, null);

        StreamHolder holder = holders.get();

        if (holder != null)
            holder.releaseIn();
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
