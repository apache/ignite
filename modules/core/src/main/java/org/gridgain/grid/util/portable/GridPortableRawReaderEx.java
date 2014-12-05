/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.portable;

import org.apache.ignite.portables.*;
import org.jetbrains.annotations.*;

/**
 * Extended reader interface.
 */
public interface GridPortableRawReaderEx extends GridPortableRawReader {
    /**
     * @return Object.
     * @throws org.apache.ignite.portables.PortableException In case of error.
     */
    @Nullable public Object readObjectDetached() throws PortableException;
}
