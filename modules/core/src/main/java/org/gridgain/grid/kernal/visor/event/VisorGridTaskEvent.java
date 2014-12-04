/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.event;

import org.apache.ignite.lang.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Lightweight counterpart for {@link GridTaskEvent}.
 */
public class VisorGridTaskEvent extends VisorGridEvent {
    /** */
    private static final long serialVersionUID = 0L;

    /** Name of the task that triggered the event. */
    private final String taskName;

    /** Name of task class that triggered the event. */
    private final String taskClassName;

    /** Task session ID. */
    private final IgniteUuid taskSessionId;

    /** Whether task was created for system needs. */
    private final boolean internal;

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
     * @param taskName Name of the task that triggered the event.
     * @param taskClassName Name of task class that triggered the event.
     * @param taskSessionId Task session ID of the task that triggered the event.
     * @param internal Whether task was created for system needs.
     */
    public VisorGridTaskEvent(
        int typeId,
        IgniteUuid id,
        String name,
        UUID nid,
        long timestamp,
        @Nullable String message,
        String shortDisplay,
        String taskName,
        String taskClassName,
        IgniteUuid taskSessionId,
        boolean internal
    ) {
        super(typeId, id, name, nid, timestamp, message, shortDisplay);

        this.taskName = taskName;
        this.taskClassName = taskClassName;
        this.taskSessionId = taskSessionId;
        this.internal = internal;
    }

    /**
     * @return Name of the task that triggered the event.
     */
    public String taskName() {
        return taskName;
    }

    /**
     * @return Name of task class that triggered the event.
     */
    public String taskClassName() {
        return taskClassName;
    }

    /**
     * @return Task session ID.
     */
    public IgniteUuid taskSessionId() {
        return taskSessionId;
    }

    /**
     * @return Whether task was created for system needs.
     */
    public boolean internal() {
        return internal;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorGridTaskEvent.class, this);
    }
}
