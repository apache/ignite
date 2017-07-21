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

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Base adapter for the events. All events (including user-defined ones) should
 * extend this adapter as it provides necessary plumbing implementation details.
 */
public class EventAdapter implements Event {
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
    private long locId = EventLocalOrder.nextOrder();

    /**
     * No-arg constructor.
     */
    public EventAdapter() {
        // No-op.
    }

    /**
     * Creates event based with given parameters.
     *
     * @param msg Optional message.
     * @param type Event type.
     */
    public EventAdapter(ClusterNode node, String msg, int type) {
        assert tstamp > 0;

        A.ensure(type > 0, "Event type ID must be greater than zero.");

        this.node = node;
        this.msg = msg;
        this.type = type;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(Event o) {
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

        return this == o || o instanceof EventAdapter && id.equals(((Event)o).id());
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
        return S.toString(EventAdapter.class, this, "name", name());
    }
}