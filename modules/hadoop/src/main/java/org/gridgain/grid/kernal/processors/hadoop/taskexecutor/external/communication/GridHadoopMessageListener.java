/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.communication;

import org.gridgain.grid.kernal.processors.hadoop.message.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.*;

/**
 * Hadoop communication message listener.
 */
public interface GridHadoopMessageListener {
    /**
     * @param desc Process descriptor.
     * @param msg Hadoop message.
     */
    public void onMessageReceived(GridHadoopProcessDescriptor desc, GridHadoopMessage msg);

    /**
     * Called when connection to remote process was lost.
     *
     * @param desc Process descriptor.
     */
    public void onConnectionLost(GridHadoopProcessDescriptor desc);
}
