/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.interop;

import org.gridgain.grid.*;
import org.gridgain.grid.util.portable.*;

/**
 * Java -> XXX interop target.
 */
public interface GridInteropFromJavaTarget {
    /**
     * @return Writer.
     * @throws GridException In case of error.
     */
    public GridPortableRawWriterEx startOutOp() throws GridException;

    /**
     * @param opType Operation type.
     * @param objPtr Object pointer.
     * @param writer Writer.
     * @throws GridException In case of error.
     */
    public void finishOutOp(int opType, long objPtr, GridPortableRawWriterEx writer) throws GridException;
}
