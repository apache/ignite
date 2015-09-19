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

package org.apache.ignite.internal.visor.event;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for lightweight counterpart for various {@link org.apache.ignite.events.Event}.
 */
public class VisorGridEvent implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Event type. */
    private final int typeId;

    /** Globally unique ID of this event. */
    private final IgniteUuid id;

    /** Name of this event. */
    private final String name;

    /** Node Id where event occurred and was recorded. */
    private final UUID nid;

    /** Event timestamp. */
    private final long ts;

    /** Event message. */
    private final String msg;

    /** Shortened version of {@code toString()} result. Suitable for humans to read. */
    private final String shortDisplay;

    /**
     * Create event with given parameters.
     *
     * @param typeId Event type.
     * @param id Event id.
     * @param name Event name.
     * @param nid Event node ID.
     * @param ts Event timestamp.
     * @param msg Event message.
     * @param shortDisplay Shortened version of {@code toString()} result.
     */
    public VisorGridEvent(int typeId, IgniteUuid id, String name, UUID nid, long ts, @Nullable String msg,
        String shortDisplay) {
        this.typeId = typeId;
        this.id = id;
        this.name = name;
        this.nid = nid;
        this.ts = ts;
        this.msg = msg;
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
    public IgniteUuid id() {
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
        return ts;
    }

    /**
     * @return Event message.
     */
    @Nullable public String message() {
        return msg;
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