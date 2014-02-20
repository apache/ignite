// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.lang;

import org.gridgain.grid.*;

/**
 * Plain closure that does not accept any arguments, returns nothing and do not implement
 * peer deploy aware.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridPlainAbsClosure {
    /**
     * Applies this closure.
     *
     * @throws GridException If error occurred.
     */
    public void apply() throws GridException;
}
