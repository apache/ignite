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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for lightweight counterpart for various {@link org.apache.ignite.events.Event}.
 */
public class VisorGridEvent extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Event type. */
    private int typeId;

    /** Globally unique ID of this event. */
    private IgniteUuid id;

    /** Name of this event. */
    private String name;

    /** Node Id where event occurred and was recorded. */
    private UUID nid;

    /** Event timestamp. */
    private long ts;

    /** Event message. */
    private String msg;

    /** Shortened version of {@code toString()} result. Suitable for humans to read. */
    private String shortDisplay;

    /**
     * Default constructor.
     */
    public VisorGridEvent() {
        // No-op.
    }

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
    public int getTypeId() {
        return typeId;
    }

    /**
     * @return Globally unique ID of this event.
     */
    public IgniteUuid getId() {
        return id;
    }

    /**
     * @return Name of this event.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Node Id where event occurred and was recorded.
     */
    public UUID getNid() {
        return nid;
    }

    /**
     * @return Event timestamp.
     */
    public long getTimestamp() {
        return ts;
    }

    /**
     * @return Event message.
     */
    @Nullable public String getMessage() {
        return msg;
    }

    /**
     * @return Shortened version of  result. Suitable for humans to read.
     */
    public String getShortDisplay() {
        return shortDisplay;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeInt(typeId);
        U.writeIgniteUuid(out, id);
        U.writeString(out, name);
        U.writeUuid(out, nid);
        out.writeLong(ts);
        U.writeString(out, msg);
        U.writeString(out, shortDisplay);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        typeId = in.readInt();
        id = U.readIgniteUuid(in);
        name = U.readString(in);
        nid = U.readUuid(in);
        ts = in.readLong();
        msg = U.readString(in);
        shortDisplay = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorGridEvent.class, this);
    }
}
