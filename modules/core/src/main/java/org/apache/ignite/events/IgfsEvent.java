/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.events;

import java.util.Map;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_IGFS_DIR_CREATED;
import static org.apache.ignite.events.EventType.EVT_IGFS_DIR_DELETED;
import static org.apache.ignite.events.EventType.EVT_IGFS_DIR_RENAMED;

/**
 * IGFS event.
 * <p>
 * Grid events are used for notification about what happens within the grid. Note that by
 * design Ignite keeps all events generated on the local node locally and it provides
 * APIs for performing a distributed queries across multiple nodes:
 * <ul>
 *      <li>
 *          {@link org.apache.ignite.IgniteEvents#remoteQuery(org.apache.ignite.lang.IgnitePredicate, long, int...)} -
 *          asynchronously querying events occurred on the nodes specified, including remote nodes.
 *      </li>
 *      <li>
 *          {@link org.apache.ignite.IgniteEvents#localQuery(org.apache.ignite.lang.IgnitePredicate, int...)} -
 *          querying only local events stored on this local node.
 *      </li>
 *      <li>
 *          {@link org.apache.ignite.IgniteEvents#localListen(org.apache.ignite.lang.IgnitePredicate, int...)} -
 *          listening to local grid events (events from remote nodes not included).
 *      </li>
 * </ul>
 * User can also wait for events using method {@link org.apache.ignite.IgniteEvents#waitForLocal(org.apache.ignite.lang.IgnitePredicate, int...)}.
 * <h1 class="header">Events and Performance</h1>
 * Note that by default all events in Ignite are enabled and therefore generated and stored
 * by whatever event storage SPI is configured. Ignite can and often does generate thousands events per seconds
 * under the load and therefore it creates a significant additional load on the system. If these events are
 * not needed by the application this load is unnecessary and leads to significant performance degradation.
 * <p>
 * It is <b>highly recommended</b> to enable only those events that your application logic requires
 * by using {@link org.apache.ignite.configuration.IgniteConfiguration#getIncludeEventTypes()} method in Ignite configuration. Note that certain
 * events are required for Ignite's internal operations and such events will still be generated but not stored by
 * event storage SPI if they are disabled in Ignite configuration.
 *
 * @see EventType#EVT_IGFS_FILE_CREATED
 * @see EventType#EVT_IGFS_FILE_RENAMED
 * @see EventType#EVT_IGFS_FILE_DELETED
 */
public class IgfsEvent extends EventAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** File path. */
    private final IgfsPath path;

    /** New file path (for RENAME event). */
    private IgfsPath newPath;

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
    public IgfsEvent(IgfsPath path, ClusterNode node, int type) {
        super(node, "IGFS event.", type);

        this.path = path;
    }

    /**
     * Constructs an event instance for path modification event
     * ({@link EventType#EVT_IGFS_FILE_RENAMED},
     * {@link EventType#EVT_IGFS_DIR_RENAMED}).
     *
     * @param path File or directory path.
     * @param newPath New file or directory path.
     * @param node Node.
     * @param type Event type.
     */
    public IgfsEvent(IgfsPath path, IgfsPath newPath, ClusterNode node, int type) {
        this(path, node, type);

        this.newPath = newPath;
    }

    /**
     * Constructs an event instance for close events:
     * ({@link EventType#EVT_IGFS_FILE_CLOSED_READ},
     * {@link EventType#EVT_IGFS_FILE_CLOSED_WRITE}).
     *
     * @param path File path.
     * @param node Node.
     * @param type Event type.
     * @param dataSize Transferred data size in bytes.
     */
    public IgfsEvent(IgfsPath path, ClusterNode node, int type, long dataSize) {
        this(path, node, type);

        this.dataSize = dataSize;
    }

    /**
     * Constructs an event instance for file metadata update events
     * ({@link EventType#EVT_IGFS_META_UPDATED}).
     *
     * @param path File path.
     * @param node Node.
     * @param type Event type.
     * @param meta Modified properties.
     */
    public IgfsEvent(IgfsPath path, ClusterNode node, int type, Map<String, String> meta) {
        this(path, node, type);

        this.meta = meta;
    }

    /**
     * Path of the file or directory, on which event has occurred.
     *
     * @return File path.
     */
    public IgfsPath path() {
        return path;
    }

    /**
     * New file or directory path for this event (used in
     * {@link EventType#EVT_IGFS_FILE_RENAMED} event).
     *
     * @return New file or directory path or {@code null},
     *         if not relevant for this event.
     */
    @Nullable public IgfsPath newPath() {
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

        return t == EVT_IGFS_DIR_CREATED || t == EVT_IGFS_DIR_RENAMED || t == EVT_IGFS_DIR_DELETED;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgfsEvent.class, this, super.toString());
    }
}