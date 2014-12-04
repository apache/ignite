/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

import net.sf.json.*;
import org.apache.ignite.compute.*;
import org.gridgain.grid.*;

import java.util.*;

import static org.apache.ignite.compute.GridComputeJobResultPolicy.*;

/**
 * Test task summarizes length of all strings in the arguments list.
 * <p>
 * The argument of the task is JSON-serialized array of objects to calculate string length sum of.
 */
public class GridClientHttpTask extends GridComputeTaskSplitAdapter<String, Integer> {
    /** Task delegate. */
    private final GridClientTcpTask delegate = new GridClientTcpTask();

    /** {@inheritDoc} */
    @Override protected Collection<? extends ComputeJob> split(int gridSize, String arg) throws GridException {
        JSON json = JSONSerializer.toJSON(arg);

        List list = json.isArray() ? JSONArray.toList((JSONArray)json, String.class, new JsonConfig()) : null;

        //noinspection unchecked
        return delegate.split(gridSize, list);
    }

    /** {@inheritDoc} */
    @Override public Integer reduce(List<ComputeJobResult> results) throws GridException {
        return delegate.reduce(results);
    }

    /** {@inheritDoc} */
    @Override public GridComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws GridException {
        if (res.getException() != null)
            return FAILOVER;

        return WAIT;
    }
}
