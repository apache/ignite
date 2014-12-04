/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor;

import org.apache.ignite.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Base class for Visor tasks intended to query data from a single node.
 */
public abstract class VisorOneNodeTask<A, R> extends VisorMultiNodeTask<A, R, R> {
    /** {@inheritDoc} */
    @Nullable @Override public R reduce(List<GridComputeJobResult> results) throws GridException {
        assert results.size() == 1;

        GridComputeJobResult res = F.first(results);

        if (res.getException() == null)
            return res.getData();

        throw res.getException();
    }
}
