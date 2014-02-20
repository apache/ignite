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
 * Plain closure that takes one input argument and does not implement {@code GridPeerDeployAware}.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridPlainInClosure<T> {
    /**
     * @param arg Closure argument.
     * @throws GridException If error occurred.
     */
    public void apply(T arg) throws GridException;
}
