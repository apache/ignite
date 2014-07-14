/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.kernal.processors.portable.*;
import org.gridgain.grid.portable.*;
import org.gridgain.portable.*;
import org.jetbrains.annotations.*;

/**
 * {@link GridPortables} implementation.
 */
public class GridPortablesImpl implements GridPortables {
    /** */
    private GridPortableProcessor proc;

    /**
     * @param proc Portable processor.
     */
    public GridPortablesImpl(GridPortableProcessor proc) {
        this.proc = proc;
    }

    /** {@inheritDoc} */
    @Override public <T> T toPortable(@Nullable Object obj) throws GridPortableException {
        return (T)proc.marshalToPortable(obj);
    }
}
