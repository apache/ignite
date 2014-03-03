/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.job;

import java.util.*;

/**
 * Job event listener.
 *
 * @author @java.author
 * @version @java.version
 */
interface GridJobEventListener extends EventListener {
    /**
     * @param worker Started job worker.
     */
    public void onJobStarted(GridJobWorker worker);

    /**
     * @param worker Job worker.
     */
    public void onBeforeJobResponseSent(GridJobWorker worker);

    /**
     * @param worker Finished job worker.
     */
    public void onJobFinished(GridJobWorker worker);
}
