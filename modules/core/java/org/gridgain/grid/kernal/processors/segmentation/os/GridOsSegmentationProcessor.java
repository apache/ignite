// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.segmentation.os;

import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.kernal.processors.segmentation.*;

/**
 * No-op implementation for {@link GridSegmentationProcessor}.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridOsSegmentationProcessor extends GridProcessorAdapter implements GridSegmentationProcessor {
    /**
     * @param ctx Kernal context.
     */
    public GridOsSegmentationProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public boolean isValidSegment() {
        return true;
    }
}
