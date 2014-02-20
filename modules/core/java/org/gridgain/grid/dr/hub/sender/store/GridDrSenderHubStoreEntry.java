// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr.hub.sender.store;

/**
 * Data center replication sender hub store entry.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridDrSenderHubStoreEntry {
    /**
     * @return Prepared external request.
     */
    public byte[] data();

    /**
     * Acknowledge.
     */
    public void acknowledge();
}
