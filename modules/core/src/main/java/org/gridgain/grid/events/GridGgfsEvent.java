/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.events;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.events.GridEventType.*;

/**
 * GGFS event.
 * <p>
 * Grid events are used for notification about what happens within the grid. Note that by
 * design GridGain keeps all events generated on the local node locally and it provides
 * APIs for performing a distributed queries across multiple nodes:
 * <ul>
 *      <li>
 *          {@link GridEvents#remoteQuery(org.gridgain.grid.lang.IgnitePredicate, long, int...)} -
 *          asynchronously querying events occurred on the nodes specified, including remote nodes.
 *      </li>
 *      <li>
 *          {@link GridEvents#localQuery(org.gridgain.grid.lang.IgnitePredicate, int...)} -
 *          querying only local events stored on this local node.
 *      </li>
 *      <li>
 *          {@link GridEvents#localListen(org.gridgain.grid.lang.IgnitePredicate, int...)} -
 *          listening to local grid events (events from remote nodes not included).
 *      </li>
 * </ul>
 * User can also wait for events using method {@link GridEvents#waitForLocal(org.gridgain.grid.lang.IgnitePredicate, int...)}.
 * <h1 class="header">Events and Performance</h1>
 * Note that by default all events in GridGain are enabled and therefore generated and stored
 * by whatever event storage SPI is configured. GridGain can and often does generate thousands events per seconds
 * under the load and therefore it creates a significant additional load on the system. If these events are
 * not needed by the application this load is unnecessary and leads to significant performance degradation.
 * <p>
 * It is <b>highly recommended</b> to enable only those events that your application logic requires
 * by using {@link org.apache.ignite.configuration.IgniteConfiguration#getIncludeEventTypes()} method in GridGain configuration. Note that certain
 * events are required for GridGain's internal operations and such events will still be generated but not stored by
 * event storage SPI if they are disabled in GridGain configuration.
 *
 * @see GridEventType#EVT_GGFS_FILE_CREATED
 * @see GridEventType#EVT_GGFS_FILE_RENAMED
 * @see GridEventType#EVT_GGFS_FILE_DELETED
 */
public class GridGgfsEvent extends GridEventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** File path. */
    private final GridGgfsPath path;

    /** New file path (for RENAME event). */
    private GridGgfsPath newPath;

    /** Data size (for data transfer events). */
    private long dataSize;

    /** Updated metadata properties (for metadata update events). */
    @GridToStringInclude
    private Map<String, String> meta;

    /**
     * Constructs an event instance.
     *
     * @param path File or directory path.
     * @param node Node.
     * @param type Event type.
     */
    public GridGgfsEvent(GridGgfsPath path, ClusterNode node, int type) {
        super(node, "GGFS event.", type);

        this.path = path;
    }

    /**
     * Constructs an event instance for path modification event
     * ({@link GridEventType#EVT_GGFS_FILE_RENAMED},
     * {@link GridEventType#EVT_GGFS_DIR_RENAMED}).
     *
     * @param path File or directory path.
     * @param newPath New file or directory path.
     * @param node Node.
     * @param type Event type.
     */
    public GridGgfsEvent(GridGgfsPath path, GridGgfsPath newPath, ClusterNode node, int type) {
        this(path, node, type);

        this.newPath = newPath;
    }

    /**
     * Constructs an event instance for close events:
     * ({@link GridEventType#EVT_GGFS_FILE_CLOSED_READ},
     * {@link GridEventType#EVT_GGFS_FILE_CLOSED_WRITE}).
     *
     * @param path File path.
     * @param node Node.
     * @param type Event type.
     * @param dataSize Transferred data size in bytes.
     */
    public GridGgfsEvent(GridGgfsPath path, ClusterNode node, int type, long dataSize) {
        this(path, node, type);

        this.dataSize = dataSize;
    }

    /**
     * Constructs an event instance for file metadata update events
     * ({@link GridEventType#EVT_GGFS_META_UPDATED}).
     *
     * @param path File path.
     * @param node Node.
     * @param type Event type.
     * @param meta Modified properties.
     */
    public GridGgfsEvent(GridGgfsPath path, ClusterNode node, int type, Map<String, String> meta) {
        this(path, node, type);

        this.meta = meta;
    }

    /**
     * Path of the file or directory, on which event has occurred.
     *
     * @return File path.
     */
    public GridGgfsPath path() {
        return path;
    }

    /**
     * New file or directory path for this event (used in
     * {@link GridEventType#EVT_GGFS_FILE_RENAMED} event).
     *
     * @return New file or directory path or {@code null},
     *         if not relevant for this event.
     */
    @Nullable public GridGgfsPath newPath() {
        return newPath;
    }

    /**
     * Transferred data size for this event.
     *
     * @return Transferred data size in bytes.
     */
    public long dataSize() {
        return dataSize;
    }

    /**
     * Updated file metadata properties.
     *
     * @return Updated metadata properties or {@code null},
     *         if not relevant for this event.
     */
    @Nullable public Map<String, String> updatedMeta() {
        return meta;
    }

    /**
     * Checks if this is a directory-related event.
     *
     * @return {@code True} if this event is directory-related.
     */
    public boolean isDirectory() {
        int t = type();

        return t == EVT_GGFS_DIR_CREATED || t == EVT_GGFS_DIR_RENAMED || t == EVT_GGFS_DIR_DELETED;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridGgfsEvent.class, this, super.toString());
    }
}
