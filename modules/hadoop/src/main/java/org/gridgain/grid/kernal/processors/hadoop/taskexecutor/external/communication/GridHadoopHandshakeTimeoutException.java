/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.communication;

import org.gridgain.grid.*;

/** Internal exception class for proper timeout handling. */
class GridHadoopHandshakeTimeoutException extends GridException {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * @param msg Message.
     */
    GridHadoopHandshakeTimeoutException(String msg) {
        super(msg);
    }
}
