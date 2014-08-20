/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop;

import org.gridgain.grid.*;

/**
 * GGFS input stream event listener.
 */
public interface GridGgfsHadoopStreamEventListener {
    /**
     * Callback invoked when the stream is being closed.
     *
     * @throws GridException If failed.
     */
    public void onClose() throws GridException;

    /**
     * Callback invoked when remote error occurs.
     *
     * @param errMsg Error message.
     */
    public void onError(String errMsg);
}
