/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.interop;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.portable.*;

/**
 * Interop cache wrapper.
 */
public interface GridInteropCache {
    /** */
    public static final int OP_GET = 0;

    /** */
    public static final int OP_GET_ASYNC = 1;

    /** */
    public static final int OP_GET_ALL = 2;

    /** */
    public static final int OP_GET_ALL_ASYNC = 3;

    /** */
    public static final int OP_PUT = 4;

    /** */
    public static final int OP_PUT_ASYNC = 5;

    /** */
    public static final int OP_PUTX = 6;

    /** */
    public static final int OP_PUTX_ASYNC = 7;

    /** */
    public static final int OP_PUT_ALL = 8;

    /** */
    public static final int OP_RELOAD = 9;

    /** */
    public static final int OP_RELOAD_ASYNC = 10;

    /** */
    public static final int OP_RELOAD_ALL = 11;

    /** */
    public static final int OP_RELOAD_ALL_ASYNC = 12;

    /** */
    public static final int OP_PEEK = 13;

    /** */
    public static final int OP_PUT_IF_ABSENT = 14;

    /** */
    public static final int OP_PUT_IF_ABSENT_ASYNC = 15;

    /** */
    public static final int OP_PUTX_IF_ABSENT = 16;

    /** */
    public static final int OP_PUTX_IF_ABSENT_ASYNC = 17;

    /** */
    public static final int OP_REPLACE_1 = 18;

    /** */
    public static final int OP_REPLACE_1_ASYNC = 19;

    /** */
    public static final int OP_REPLACEX_1 = 20;

    /** */
    public static final int OP_REPLACEX_1_ASYNC = 21;

    /** */
    public static final int OP_REPLACE_2 = 22;

    /** */
    public static final int OP_REPLACE_2_ASYNC = 23;

    /** */
    public static final int OP_EVICT = 24;

    /** */
    public static final int OP_EVICT_ALL = 25;

    /** */
    public static final int OP_CLEAR = 26;

    /** */
    public static final int OP_CLEAR_ALL = 27;

    /** */
    public static final int OP_COMPACT = 28;

    /** */
    public static final int OP_COMPACT_ALL = 29;

    /** */
    public static final int OP_REMOVE = 30;

    /** */
    public static final int OP_REMOVE_ASYNC = 31;

    /** */
    public static final int OP_REMOVEX = 32;

    /** */
    public static final int OP_REMOVEX_ASYNC = 33;

    /** */
    public static final int OP_REMOVE_ALL = 34;

    /** */
    public static final int OP_REMOVE_ALL_ASYNC = 35;

    /** */
    public static final int OP_LOCK = 36;

    /** */
    public static final int OP_LOCK_ASYNC = 37;

    /** */
    public static final int OP_LOCK_ALL = 38;

    /** */
    public static final int OP_LOCK_ALL_ASYNC = 39;

    /** */
    public static final int OP_UNLOCK = 40;

    /** */
    public static final int OP_UNLOCK_ALL = 41;

    /** */
    public static final int OP_PROMOTE = 42;

    /** */
    public static final int OP_PROMOTE_ALL = 43;

    /** */
    public static final int OP_NAME = 44;

    /**
     * Synchronous IN operation.
     *
     * @param opType Operation type.
     * @param in Input stream.
     * @return Value specific for the given operation otherwise.
     * @throws GridException In case of failure.
     */
    public int inOp(int opType, GridPortableInputStream in) throws GridException;

    /**
     * Synchronous OUT operation.
     *
     * @param opType Operation type.
     * @param stream Native stream address.
     * @param arr Native array address.
     * @param cap Capacity.
     */
    public void outOp(int opType, long stream, long arr, int cap) throws GridException;

    /**
     * Synchronous IN-OUT operation.
     *
     * @param opType Operation type.
     * @param in Input stream.
     * @param out Output stream.
     * @throws GridException In case of failure.
     */
    public void inOutOp(int opType, GridPortableInputStream in, GridPortableOutputStream out) throws GridException;

    /**
     * Asynchronous IN operation.
     *
     * @param opType Operation type.
     * @param in Input stream.
     * @param futId Future ID.
     * @throws GridException In case of failure.
     */
    public void inOpAsync(int opType, GridPortableInputStream in, long futId) throws GridException;

    /**
     * Asynchronous IN-OUT operation.
     *
     * @param opType Operation type.
     * @param in Input stream.
     * @param out Output stream.
     * @param futId Future ID.
     * @throws GridException In case of failure.
     */
    public void inOutOpAsync(int opType, GridPortableInputStream in, GridPortableOutputStream out, long futId)
        throws GridException;
}
