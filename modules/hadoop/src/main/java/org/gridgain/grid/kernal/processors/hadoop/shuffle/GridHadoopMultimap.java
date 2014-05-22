/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.shuffle;

import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Multimap for hadoop intermediate results input/output.
 */
public interface GridHadoopMultimap extends AutoCloseable {
    /**
     * Incrementally visits all the keys and values in the map.
     *
     * @param ignoreLastVisited Flag indicating that visiting must be started from the beginning.
     * @param v Visitor.
     * @return {@code false} If visiting was impossible.
     */
    public boolean visit(boolean ignoreLastVisited, Visitor v) throws GridException;

    /**
     * @return Adder.
     * @throws GridException If failed.
     */
    public Adder startAdding() throws GridException;

    /**
     * @return Task input.
     * @throws GridException If failed.
     */
    public GridHadoopTaskInput input() throws GridException;

    /** {@inheritDoc} */
    @Override public void close();

    /**
     * Adder.
     */
    public interface Adder extends AutoCloseable {
        /**
         * Adds value for the given key.
         *
         * @param key Key.
         * @param val Value.
         * @return Meta pointer for the key.
         */
        public long add(Object key, Object val) throws GridException;

        /**
         * @param in Data input.
         * @param reuse Reusable key.
         * @return Key.
         * @throws GridException If failed.
         */
        public Key addKey(DataInput in, @Nullable Key reuse) throws GridException;

        /** {@inheritDoc} */
        public void close() throws GridException;
    }

    /**
     * Key add values to.
     */
    public interface Key {
        /**
         * @param val Value.
         */
        public void add(Value val);
    }

    /**
     * Value.
     */
    public interface Value {
        /**
         * @return Size in bytes.
         */
        public int size();

        /**
         * @param ptr Pointer.
         */
        public void copyTo(long ptr);
    }

    /**
     * Key and values visitor.
     */
    public interface Visitor {
        /**
         * @param keyPtr Key pointer.
         * @param keySize Key size.
         */
        public void onKey(long keyPtr, int keySize) throws GridException;

        /**
         * @param valPtr Value pointer.
         * @param valSize Value size.
         */
        public void onValue(long valPtr, int valSize) throws GridException;
    }
}
