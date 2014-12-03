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
import org.jetbrains.annotations.*;

/**
 * Interop target abstraction.
 */
public interface GridInteropTarget {
    /**
     * Synchronous IN operation.
     *
     * @param type Operation type.
     * @param stream Input stream.
     * @return Value specific for the given operation otherwise.
     * @throws GridException In case of failure.
     */
    public int inOp(int type, GridPortableInputStream stream) throws GridException;

    /**
     * Synchronous IN operation which returns managed object as result.
     *
     * @param type Operation type.
     * @param stream Input stream.
     * @return Managed result.
     * @throws GridException If case of failure.
     */
    public Object inOpObject(int type, GridPortableInputStream stream) throws GridException;

    /**
     * Synchronous OUT operation.
     *
     * @param type Operation type.
     * @param stream Native stream address.
     * @param arr Native array address.
     * @param cap Capacity.
     * @throws GridException In case of failure.
     */
    public void outOp(int type, long stream, long arr, int cap) throws GridException;

    /**
     * Synchronous IN-OUT operation.
     *
     * @param type Operation type.
     * @param inStream Input stream.
     * @param outStream Native stream address.
     * @param outArr Native array address.
     * @param outCap Capacity.
     * @throws GridException In case of failure.
     */
    public void inOutOp(int type, GridPortableInputStream inStream, long outStream, long outArr, int outCap)
        throws GridException;

    /**
     * Synchronous IN-OUT operation with optional argument.
     *
     * @param type Operation type.
     * @param inStream Input stream.
     * @param outStream Native stream address.
     * @param outArr Native array address.
     * @param outCap Capacity.
     * @param arg Argument (optional).
     * @throws GridException In case of failure.
     */
    public void inOutOp(int type, GridPortableInputStream inStream, long outStream, long outArr, int outCap,
        @Nullable Object arg) throws GridException;

    /**
     * Asynchronous IN operation.
     *
     * @param type Operation type.
     * @param futId Future ID.
     * @param in Input stream.
     * @throws GridException In case of failure.
     */
    public void inOpAsync(int type, long futId, GridPortableInputStream in) throws GridException;

    /**
     * Asynchronous IN-OUT operation.
     *
     * @param type Operation type.
     * @param futId Future ID.
     * @param in Input stream.
     * @param outStream Native stream address.
     * @param outArr Native array address.
     * @param outCap Capacity.
     * @throws GridException In case of failure.
     */
    public void inOutOpAsync(int type, long futId, GridPortableInputStream in, long outStream, long outArr, int outCap)
        throws GridException;
}
