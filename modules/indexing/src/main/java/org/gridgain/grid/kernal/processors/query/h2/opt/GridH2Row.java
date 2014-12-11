/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2.opt;

import org.h2.result.*;
import org.h2.value.*;

/**
 * Row with locking support needed for unique key conflicts resolution.
 */
public class GridH2Row extends Row implements GridSearchRowPointer {
    /**
     * @param data Column values.
     */
    public GridH2Row(Value... data) {
        super(data, MEMORY_CALCULATE);
    }

    /** {@inheritDoc} */
    @Override public long pointer() {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public void incrementRefCount() {
        throw new IllegalStateException();
    }

    /** {@inheritDoc} */
    @Override public void decrementRefCount() {
        throw new IllegalStateException();
    }
}
