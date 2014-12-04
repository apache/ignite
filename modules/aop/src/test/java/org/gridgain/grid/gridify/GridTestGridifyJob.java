/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.gridify;

import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;

import java.io.*;

/**
 * Test gridify job.
 */
public class GridTestGridifyJob extends ComputeJobAdapter {
    /** */
    @IgniteLoggerResource
    private IgniteLogger log;

    /**
     * @param arg Argument.
     */
    public GridTestGridifyJob(String arg) {
        super(arg);
    }

    /** {@inheritDoc} */
    @Override public Serializable execute() throws GridException {
        if (log.isInfoEnabled())
            log.info("Execute GridTestGridifyJob.execute(" + argument(0) + ')');

        GridTestAopTarget target = new GridTestAopTarget();

        try {
            if ("1".equals(argument(0)))
                return target.gridifyNonDefaultClass("10");
            else if ("2".equals(argument(0)))
                return target.gridifyNonDefaultName("20");
            else if ("3".equals(argument(0)))
                return target.gridifyNonDefaultClassResource("30");
            else if ("4".equals(argument(0)))
                return target.gridifyNonDefaultNameResource("40");
        }
        catch (GridTestGridifyException e) {
            throw new RuntimeException("Failed to execute target method.", e);
        }

        assert false : "Argument must be equals to \"0\" [gridifyArg=" + argument(0) + ']';

        // Never reached.
        return null;
    }
}
