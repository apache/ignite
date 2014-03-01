// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.events;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Base adapter for the events. All events (including user-defined ones) should
 * extend this adapter as it provides necessary plumbing implementation details.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridEventAdapter implements GridEvent {
    /** */
    private final GridUuid id = GridUuid.randomUuid();

    /** */
    private final long tstamp = U.currentTimeMillis();

    /** */
    private UUID nodeId;

    /** */
    private String msg;

    /** */
    private int type;

    /** */
    private long locId = GridEventLocalOrder.nextOrder();

    /**
     * No-arg constructor.
     */
    public GridEventAdapter() {
        // No-op.
    }

    /**
     * Creates event based with given parameters.
     *
     * @param nodeId Node ID.
     * @param msg Optional message.
     * @param type Event type.
     */
    public GridEventAdapter(UUID nodeId, String msg, int type) {
        assert nodeId != null;
        assert tstamp > 0;

        A.ensure(type > 0, "Event type ID must be greater than zero.");

        this.nodeId = nodeId;
        this.msg = msg;
        this.type = type;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(GridEvent o) {
        return o == null ? 1 : id.compareTo(o.id());
    }

    /** {@inheritDoc} */
    @Override public GridUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public long localOrder() {
        return locId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        assert id != null;

        return this == o || o instanceof GridEventAdapter && id.equals(((GridEvent)o).id());
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        assert id != null;

        return id.hashCode();
    }

    /**
     * Sets ID of the node where even is occurred (i.e. node local to the event).
     *
     * @param nodeId Node ID.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * Sets optional event message.
     *
     * @param msg Optional event message.
     */
    public void message(@Nullable String msg) {
        this.msg = msg;
    }

    /**
     * Sets event type.
     *
     * @param type Event type.
     */
    public void type(int type) {
        this.type = type;
    }

    /** {@inheritDoc} */
    @Override public UUID nodeId() {
        return nodeId;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String message() {
        return msg;
    }

    /** {@inheritDoc} */
    @Override public int type() {
        return type;
    }

    /** {@inheritDoc} */
    @Override public long timestamp() {
        return tstamp;
    }

    /**
     * Gets event type name.
     *
     * @return Event type name.
     */
    @Override public String name() {
        return U.gridEventName(type());
    }

    /** {@inheritDoc} */
    @Override public String shortDisplay() {
        return toString();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridEventAdapter.class, this, "name", name());
    }
}
