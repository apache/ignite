/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.marshaller.optimized;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.io.*;

import java.io.*;
import java.util.concurrent.*;

/**
 * Storage for object streams.
 */
class GridOptimizedObjectStreamRegistry {
    /** Holders. */
    private static final ThreadLocal<StreamHolder> holders = new ThreadLocal<>();

    /** Holders pool. */
    private static BlockingQueue<StreamHolder> pool;

    /**
     * Ensures singleton.
     */
    private GridOptimizedObjectStreamRegistry() {
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
     * @throws GridInterruptedException If thread is interrupted while trying to take holder from pool.
     */
    static GridOptimizedObjectOutputStream out() throws GridInterruptedException {
        return holder().acquireOut();
    }

    /**
     * Gets input stream.
     *
     * @return Object input stream.
     * @throws GridInterruptedException If thread is interrupted while trying to take holder from pool.
     */
    static GridOptimizedObjectInputStream in() throws GridInterruptedException {
        return holder().acquireIn();
    }

    /**
     * Closes and releases output stream.
     *
     * @param out Object output stream.
     */
    static void closeOut(GridOptimizedObjectOutputStream out) {
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
    static void closeIn(GridOptimizedObjectInputStream in) {
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
     * @throws GridInterruptedException If thread is interrupted while trying to take holder from pool.
     */
    private static StreamHolder holder() throws GridInterruptedException {
        StreamHolder holder = holders.get();

        if (holder == null) {
            try {
                holders.set(holder = pool != null ? pool.take() : new StreamHolder());
            }
            catch (InterruptedException e) {
                throw new GridInterruptedException("Failed to take object stream from pool (thread interrupted).", e);
            }
        }

        return holder;
    }

    /**
     * Streams holder.
     */
    private static class StreamHolder {
        /** Output stream. */
        private final GridOptimizedObjectOutputStream out = createOut();

        /** Input stream. */
        private final GridOptimizedObjectInputStream in = createIn();

        /** Output streams counter. */
        private int outAcquireCnt;

        /** Input streams counter. */
        private int inAcquireCnt;

        /**
         * Gets output stream.
         *
         * @return Object output stream.
         */
        GridOptimizedObjectOutputStream acquireOut() {
            return outAcquireCnt++ > 0 ? createOut() : out;
        }

        /**
         * Gets input stream.
         *
         * @return Object input stream.
         */
        GridOptimizedObjectInputStream acquireIn() {
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
        private GridOptimizedObjectOutputStream createOut() {
            try {
                return new GridOptimizedObjectOutputStream(new GridUnsafeDataOutput(4 * 1024));
            }
            catch (IOException e) {
                throw new GridRuntimeException("Failed to create object output stream.", e);
            }
        }

        /**
         * Creates input stream.
         *
         * @return Object input stream.
         */
        private GridOptimizedObjectInputStream createIn() {
            try {
                return new GridOptimizedObjectInputStream(new GridUnsafeDataInput());
            }
            catch (IOException e) {
                throw new GridRuntimeException("Failed to create object input stream.", e);
            }
        }
    }
}
