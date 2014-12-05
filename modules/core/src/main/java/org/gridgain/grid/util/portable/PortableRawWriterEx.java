/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.portable;

import org.apache.ignite.portables.*;
import org.gridgain.grid.kernal.processors.portable.*;
import org.jetbrains.annotations.*;

/**
 * Extended writer interface.
 */
public interface PortableRawWriterEx extends PortableRawWriter, AutoCloseable {
    /**
     * @param obj Object to write.
     * @throws org.apache.ignite.portables.PortableException In case of error.
     */
    public void writeObjectDetached(@Nullable Object obj) throws PortableException;

    /**
     * @return Output stream.
     */
    public GridPortableOutputStream out();

    /**
     * Cleans resources.
     */
    @Override public void close();
}
