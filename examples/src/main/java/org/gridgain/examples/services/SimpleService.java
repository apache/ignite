/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.services;

import org.gridgain.grid.service.*;

/**
 * Simple service which loops infinitely and prints out a counter.
 */
public class SimpleService implements GridService {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override public void cancel(GridServiceContext ctx) {
        System.out.println("Service was cancelled: " + ctx.name());
    }

    /** {@inheritDoc} */
    @Override public void execute(GridServiceContext ctx) throws Exception {
        System.out.println("Deployed distributed service: " + ctx.name());

        int cntr = 1;

        while (!ctx.isCancelled()) {
            System.out.println("Distributed service '" + ctx.name() + "' iteration #" + cntr++);

            Thread.sleep(3000);
        }
    }
}
