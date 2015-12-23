package org.apache.ignite.hadoop.fs.v2;

import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.fs.AbstractFileSystem;

/**
 * This factory is {@link Serializable} because it should be transferable over the network.
 */
interface HadoopAbstractFileSystemFactory extends Serializable {
    /**
     * Gets the file system, possibly creating it or taking a cached instance.
     * All the other data needed for the file system creation are expected to be contained
     * in this object instance.
     *
     * @param userName The user name
     * @return The file system.
     * @throws IOException On error.
     */
    public AbstractFileSystem get(String userName) throws IOException;
}
