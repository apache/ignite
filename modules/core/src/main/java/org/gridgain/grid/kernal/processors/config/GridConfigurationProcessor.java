/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.config;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.*;
import org.jetbrains.annotations.*;

/**
 * Configuration processor.
 */
public class GridConfigurationProcessor extends GridProcessorAdapter {
    /**
     * @param ctx Kernal context.
     */
    public GridConfigurationProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param ctx Kernal context.
     * @return Configuration processor.
     */
    public static GridConfigurationProcessor instance(GridKernalContext ctx) {
        return new GridConfigurationProcessor(ctx);
    }

    /**
     * @param cfgPath Configuration path.
     * @return Grid configuration.
     * @throws GridException If failed.
     */
    public GridConfiguration loadConfiguration(@Nullable String cfgPath) throws GridException {
        throw new GridException("Default configuration processor is not able to load configuration: " + cfgPath);
    }
}
