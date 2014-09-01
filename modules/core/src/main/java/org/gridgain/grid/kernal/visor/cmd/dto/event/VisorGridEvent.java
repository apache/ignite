/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.event;

import org.gridgain.grid.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Base class for lightweight counterpart for various {@link GridEvent}.
 */
public class VisorGridEvent implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Event type. */
    private final int typeId;

    /** Globally unique ID of this event. */
    private final GridUuid id;

    /** Name of this event. */
    private final String name;

    /** Node Id where event occurred and was recorded. */
    private final UUID nid;

    /** Event timestamp. */
    private final long timestamp;

    /** Event message. */
    private final String message;

    /** Shortened version of {@code toString()} result. Suitable for humans to read. */
    private final String shortDisplay;

    /**
     * Create event with given parameters.
     *
     * @param typeId Event type.
     * @param id Event id.
     * @param name Event name.
     * @param nid Event node ID.
     * @param timestamp Event timestamp.
     * @param message Event message.
     * @param shortDisplay Shortened version of {@code toString()} result.
     */
    public VisorGridEvent(int typeId, GridUuid id, String name, UUID nid, long timestamp, @Nullable String message,
        String shortDisplay) {
        this.typeId = typeId;
        this.id = id;
        this.name = name;
        this.nid = nid;
        this.timestamp = timestamp;
        this.message = message;
        this.shortDisplay = shortDisplay;
    }

    /**
     * @return Event type.
     */
    public int typeId() {
        return typeId;
    }

    /**
     * @return Globally unique ID of this event.
     */
    public GridUuid id() {
        return id;
    }

    /**
     * @return Name of this event.
     */
    public String name() {
        return name;
    }

    /**
     * @return Node Id where event occurred and was recorded.
     */
    public UUID nid() {
        return nid;
    }

    /**
     * @return Event timestamp.
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * @return Event message.
     */
    @Nullable public String message() {
        return message;
    }

    /**
     * @return Shortened version of  result. Suitable for humans to read.
     */
    public String shortDisplay() {
        return shortDisplay;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorGridEvent.class, this);
    }
}
