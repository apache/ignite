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

package org.apache.ignite.internal.managers.eventstorage;

import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.jetbrains.annotations.*;

import java.nio.*;
import java.util.*;

/**
 * Event storage message.
 */
public class GridEventStorageMessage extends MessageAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridDirectTransient
    private Object resTopic;

    /** */
    private byte[] resTopicBytes;

    /** */
    private byte[] filter;

    /** */
    @GridDirectTransient
    private Collection<Event> evts;

    /** */
    private byte[] evtsBytes;

    /** */
    @GridDirectTransient
    private Throwable ex;

    /** */
    private byte[] exBytes;

    /** */
    private IgniteUuid clsLdrId;

    /** */
    private DeploymentMode depMode;

    /** */
    private String filterClsName;

    /** */
    private String userVer;

    /** Node class loader participants. */
    @GridToStringInclude
    @GridDirectMap(keyType = UUID.class, valueType = IgniteUuid.class)
    private Map<UUID, IgniteUuid> ldrParties;

    /** */
    public GridEventStorageMessage() {
        // No-op.
    }

    /**
     * @param resTopic Response topic,
     * @param filter Query filter.
     * @param filterClsName Filter class name.
     * @param clsLdrId Class loader ID.
     * @param depMode Deployment mode.
     * @param userVer User version.
     * @param ldrParties Node loader participant map.
     */
    GridEventStorageMessage(
        Object resTopic,
        byte[] filter,
        String filterClsName,
        IgniteUuid clsLdrId,
        DeploymentMode depMode,
        String userVer,
        Map<UUID, IgniteUuid> ldrParties) {
        this.resTopic = resTopic;
        this.filter = filter;
        this.filterClsName = filterClsName;
        this.depMode = depMode;
        this.clsLdrId = clsLdrId;
        this.userVer = userVer;
        this.ldrParties = ldrParties;

        evts = null;
        ex = null;
    }

    /**
     * @param evts Grid events.
     * @param ex Exception occurred during processing.
     */
    GridEventStorageMessage(Collection<Event> evts, Throwable ex) {
        this.evts = evts;
        this.ex = ex;

        resTopic = null;
        filter = null;
        filterClsName = null;
        depMode = null;
        clsLdrId = null;
        userVer = null;
    }

    /**
     * @return Response topic.
     */
    Object responseTopic() {
        return resTopic;
    }

    /**
     * @param resTopic Response topic.
     */
    void responseTopic(Object resTopic) {
        this.resTopic = resTopic;
    }

    /**
     * @return Serialized response topic.
     */
    byte[] responseTopicBytes() {
        return resTopicBytes;
    }

    /**
     * @param resTopicBytes Serialized response topic.
     */
    void responseTopicBytes(byte[] resTopicBytes) {
        this.resTopicBytes = resTopicBytes;
    }

    /**
     * @return Filter.
     */
    byte[] filter() {
        return filter;
    }

    /**
     * @return Events.
     */
    @Nullable Collection<Event> events() {
        return evts != null ? Collections.unmodifiableCollection(evts) : null;
    }

    /**
     * @param evts Events.
     */
    void events(@Nullable Collection<Event> evts) {
        this.evts = evts;
    }

    /**
     * @return Serialized events.
     */
    byte[] eventsBytes() {
        return evtsBytes;
    }

    /**
     * @param evtsBytes Serialized events.
     */
    void eventsBytes(byte[] evtsBytes) {
        this.evtsBytes = evtsBytes;
    }

    /**
     * @return the Class loader ID.
     */
    IgniteUuid classLoaderId() {
        return clsLdrId;
    }

    /**
     * @return Deployment mode.
     */
    DeploymentMode deploymentMode() {
        return depMode;
    }

    /**
     * @return Filter class name.
     */
    String filterClassName() {
        return filterClsName;
    }

    /**
     * @return User version.
     */
    String userVersion() {
        return userVer;
    }

    /**
     * @return Node class loader participant map.
     */
    @Nullable Map<UUID, IgniteUuid> loaderParticipants() {
        return ldrParties != null ? Collections.unmodifiableMap(ldrParties) : null;
    }

    /**
     * @param ldrParties Node class loader participant map.
     */
    void loaderParticipants(Map<UUID, IgniteUuid> ldrParties) {
        this.ldrParties = ldrParties;
    }

    /**
     * @return Exception.
     */
    Throwable exception() {
        return ex;
    }

    /**
     * @param ex Exception.
     */
    void exception(Throwable ex) {
        this.ex = ex;
    }

    /**
     * @return Serialized exception.
     */
    byte[] exceptionBytes() {
        return exBytes;
    }

    /**
     * @param exBytes Serialized exception.
     */
    void exceptionBytes(byte[] exBytes) {
        this.exBytes = exBytes;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        GridEventStorageMessage _clone = (GridEventStorageMessage)_msg;

        _clone.resTopic = resTopic;
        _clone.resTopicBytes = resTopicBytes;
        _clone.filter = filter;
        _clone.evts = evts;
        _clone.evtsBytes = evtsBytes;
        _clone.ex = ex;
        _clone.exBytes = exBytes;
        _clone.clsLdrId = clsLdrId;
        _clone.depMode = depMode;
        _clone.filterClsName = filterClsName;
        _clone.userVer = userVer;
        _clone.ldrParties = ldrParties;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        MessageWriteState state = MessageWriteState.get();
        MessageWriter writer = state.writer();

        writer.setBuffer(buf);

        if (!state.isTypeWritten()) {
            if (!writer.writeByte(null, directType()))
                return false;

            state.setTypeWritten();
        }

        switch (state.index()) {
            case 0:
                if (!writer.writeIgniteUuid("clsLdrId", clsLdrId))
                    return false;

                state.increment();

            case 1:
                if (!writer.writeEnum("depMode", depMode))
                    return false;

                state.increment();

            case 2:
                if (!writer.writeByteArray("evtsBytes", evtsBytes))
                    return false;

                state.increment();

            case 3:
                if (!writer.writeByteArray("exBytes", exBytes))
                    return false;

                state.increment();

            case 4:
                if (!writer.writeByteArray("filter", filter))
                    return false;

                state.increment();

            case 5:
                if (!writer.writeString("filterClsName", filterClsName))
                    return false;

                state.increment();

            case 6:
                if (!writer.writeMap("ldrParties", ldrParties, UUID.class, IgniteUuid.class))
                    return false;

                state.increment();

            case 7:
                if (!writer.writeByteArray("resTopicBytes", resTopicBytes))
                    return false;

                state.increment();

            case 8:
                if (!writer.writeString("userVer", userVer))
                    return false;

                state.increment();

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        reader.setBuffer(buf);

        switch (readState) {
            case 0:
                clsLdrId = reader.readIgniteUuid("clsLdrId");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 1:
                depMode = reader.readEnum("depMode", DeploymentMode.class);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 2:
                evtsBytes = reader.readByteArray("evtsBytes");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 3:
                exBytes = reader.readByteArray("exBytes");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 4:
                filter = reader.readByteArray("filter");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 5:
                filterClsName = reader.readString("filterClsName");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 6:
                ldrParties = reader.readMap("ldrParties", UUID.class, IgniteUuid.class, false);

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 7:
                resTopicBytes = reader.readByteArray("resTopicBytes");

                if (!reader.isLastRead())
                    return false;

                readState++;

            case 8:
                userVer = reader.readString("userVer");

                if (!reader.isLastRead())
                    return false;

                readState++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 13;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridEventStorageMessage.class, this);
    }
}
