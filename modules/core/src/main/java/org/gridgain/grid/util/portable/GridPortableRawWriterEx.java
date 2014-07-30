/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.portable;

import org.gridgain.grid.portables.*;
import org.jetbrains.annotations.*;

/**
 * Extended writer interface.
 */
public interface GridPortableRawWriterEx extends GridPortableRawWriter {
    /**
     * @param obj Object to write.
     * @throws GridPortableException In case of error.
     */
    public void writeObjectDetached(@Nullable Object obj) throws GridPortableException;
}
