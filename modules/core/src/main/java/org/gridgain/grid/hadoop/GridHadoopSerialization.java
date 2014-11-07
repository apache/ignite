/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Hadoop serialization. Not thread safe object, must be created for each thread or correctly synchronized.
 */
public interface GridHadoopSerialization extends AutoCloseable {
    /**
     * Writes the given object to output.
     *
     * @param out Output.
     * @param obj Object to serialize.
     * @throws GridException If failed.
     */
    public void write(DataOutput out, Object obj) throws GridException;

    /**
     * Reads object from the given input optionally reusing given instance.
     *
     * @param in Input.
     * @param obj Object.
     * @return New object or reused instance.
     * @throws GridException If failed.
     */
    public Object read(DataInput in, @Nullable Object obj) throws GridException;

    /**
     * Finalise the internal objects.
     * 
     * @throws GridException If failed.
     */
    @Override public void close() throws GridException;
}
