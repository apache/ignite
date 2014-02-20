// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.dr.hub.sender.store;

import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

/**
 * Data center replication sender hub store cursor.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridDrSenderHubStoreCursor extends AutoCloseable {
    /**
     * Get next entry from that cursor.
     *
     * @return Next entry from cursor or {@code null} if none.
     * @throws GridException If failed.
     * @throws GridDrSenderHubStoreCorruptedException If store was corrupted.
     */
    @Nullable public GridDrSenderHubStoreEntry next() throws GridException, GridDrSenderHubStoreCorruptedException;
}
