/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.dr.os;

import org.gridgain.grid.dr.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.kernal.processors.dr.*;
import org.jetbrains.annotations.*;

/**
 * No-op implementation for {@link GridDrProcessor}.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridOsDrProcessor extends GridProcessorAdapter implements GridDrProcessor {
    /**
     * @param ctx Kernal context.
     */
    public GridOsDrProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridDr dr() {
        return null;
    }
}
