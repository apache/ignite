/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.events;

import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

/**
 * Base adapter for the events. All events (including user-defined ones) should
 * extend this adapter as it provides necessary plumbing implementation details.
 */
public class IgniteEventAdapter implements IgniteEvent {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final IgniteUuid id = IgniteUuid.randomUuid();

    /** */
    private final long tstamp = U.currentTimeMillis();

    /** */
    private ClusterNode node;

    /** */
    private String msg;

    /** */
    private int type;

    /** */
    private long locId = IgniteEventLocalOrder.nextOrder();

    /**
     * No-arg constructor.
     */
    public IgniteEventAdapter() {
        // No-op.
    }

    /**
     * Creates event based with given parameters.
     *
     * @param msg Optional message.
     * @param type Event type.
     */
    public IgniteEventAdapter(ClusterNode node, String msg, int type) {
        assert tstamp > 0;

        A.ensure(type > 0, "Event type ID must be greater than zero.");

        this.node = node;
        this.msg = msg;
        this.type = type;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(IgniteEvent o) {
        return o == null ? 1 : id.compareTo(o.id());
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public long localOrder() {
        return locId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        assert id != null;

        return this == o || o instanceof IgniteEventAdapter && id.equals(((IgniteEvent)o).id());
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        assert id != null;

        return id.hashCode();
    }

    /**
     * Sets node where even is occurred (i.e. node local to the event).
     *
     * @param node Node.
     */
    public void node(ClusterNode node) {
        this.node = node;
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
    @Override public ClusterNode node() {
        return node;
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
        return S.toString(IgniteEventAdapter.class, this, "name", name());
    }
}
