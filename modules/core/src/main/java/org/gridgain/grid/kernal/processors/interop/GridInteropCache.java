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
    /** Operation type: GET. */
    public static final int OP_GET = 0;

    /** Operation type: PUT. */
    public static final int OP_PUT = 1;

    /**
     * Get value from cache.
     *
     * @param keyAddr Address where key is stored in unmanaged memory.
     * @return Address where value is stored in unmanaged memory.
     */
    public long get(long keyAddr);

    /**
     * Get value from cache.
     *
     * @param keyAddr Address where key is stored in unmanaged memory.
     * @param cbAddr Callback address.
     * @return {@code 0} in case of success, positive value representing
     *     unmanaged memory address in case of exception.
     */
    public long getAsync(long keyAddr, long cbAddr);

    /** NEW DESIGN. */

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
     * Asynchronous IN operation.
     *
     * @param opType Operation type.
     * @param in Input stream.
     * @param futId Future ID.
     * @throws GridException In case of failure.
     */
    public void inOpAsync(int opType, GridPortableInputStream in, long futId) throws GridException;

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
