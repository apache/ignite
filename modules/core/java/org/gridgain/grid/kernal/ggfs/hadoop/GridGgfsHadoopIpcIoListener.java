/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.hadoop;

/**
 * Listens to the events of {@link GridGgfsHadoopIpcIo}.
 */
public interface GridGgfsHadoopIpcIoListener {
    /**
     * Callback invoked when the IO is being closed.
     */
    public void onClose();

    /**
     * Callback invoked when remote error occurs.
     *
     * @param streamId Stream ID.
     * @param errMsg Error message.
     */
    public void onError(long streamId, String errMsg);
}
