/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.test.gridify;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.compute.gridify.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;

import java.io.*;
import java.util.*;

/**
 * External gridify task.
 */
public class GridExternalGridifyTask extends GridComputeTaskSplitAdapter<GridifyArgument, Object> {
    /** */
    public static final String TASK_NAME = "org.test.gridify.GridExternalGridifyTask";

    /** {@inheritDoc} */
    @Override public Collection<? extends GridComputeJob> split(int gridSize, GridifyArgument arg) throws GridException {
        assert arg.getMethodParameters().length == 1;

        return Collections.singletonList(new GridComputeJobAdapter((String)arg.getMethodParameters()[0]) {
            /** */
            @GridLoggerResource private GridLogger log;

            /** {@inheritDoc} */
            @Override public Serializable execute() {
                if (log.isInfoEnabled()) {
                    log.info("Execute GridTestGridifyJob.execute(" + argument(0) + ')');
                }

                GridExternalAopTarget target = new GridExternalAopTarget();

                try {
                    if ("1".equals(argument(0))) {
                        return target.gridifyNonDefaultClass("10");
                    }
                    else if ("2".equals(argument(0))) {
                        return target.gridifyNonDefaultName("20");
                    }
                    else if ("3".equals(argument(0))) {
                        return target.gridifyNonDefaultClassResource("30");
                    }
                    else if ("4".equals(argument(0))) {
                        return target.gridifyNonDefaultNameResource("40");
                    }
                }
                catch (GridExternalGridifyException e) {
                    throw new RuntimeException("Failed to execute target method.", e);
                }

                assert false : "Argument must be equals to \"0\" [gridifyArg=" + argument(0) + ']';

                // Never reached.
                return null;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
        assert results.size() == 1;

        return results.get(0).getData();
    }
}
