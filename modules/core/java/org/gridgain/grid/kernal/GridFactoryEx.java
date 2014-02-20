// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

/**
 * Extended grid factory.
 *
 * @author @java.author
 * @version @java.version
 */
@SuppressWarnings("ExtendsUtilityClass")
public class GridFactoryEx extends GridGain {
    /**
     * Gets grid instance without waiting its initialization.
     *
     * @param name Grid name.
     * @return Grid instance.
     */
    public static GridKernal gridx(@Nullable String name) {
        GridNamedInstance grid = name != null ? grids.get(name) : dfltGrid;

        GridKernal res = null;

        if (grid == null || (res = grid.gridx()) == null)
            U.warn(null, "Grid instance was not properly started or was already stopped: " + name);

        return res;
    }
}
