/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nodestart;

/**
 * Implementation of {@link GridSshProcessor}.
 */
public class GridSshProcessorImpl implements GridSshProcessor {
    /** {@inheritDoc} */
    @Override public GridNodeCallable nodeStartCallable(GridRemoteStartSpecification spec, int timeout) {
        return new GridNodeCallableImpl(spec, timeout);
    }
}
