/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr.hub.sender.store;

import org.gridgain.grid.*;

/**
 * Exception thrown in case data center replication sender hub store was corrupted.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridDrSenderHubStoreCorruptedException extends GridException {
    /**
     * Constructor.
     *
     * @param msg Message.
     */
    public GridDrSenderHubStoreCorruptedException(String msg) {
        super(msg);
    }
}
