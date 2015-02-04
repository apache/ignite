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

import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Base class for lightweight counterpart for various {@link org.apache.ignite.events.IgniteEvent}.
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
    public VisorGridEvent(int typeId, IgniteUuid id, String name, UUID nid, long timestamp, @Nullable String message,
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
