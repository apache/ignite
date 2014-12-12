/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.ignite.*;
import org.gridgain.grid.*;

/**
 * Exception that throws when the task is cancelling.
 */
public class GridHadoopTaskCancelledException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @param msg Exception message.
     */
    public GridHadoopTaskCancelledException(String msg) {
        super(msg);
    }
}
