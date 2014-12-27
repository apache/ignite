/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.eventstorage;

import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.nio.*;
import java.util.*;

/**
 * Event storage message.
 */
public class GridEventStorageMessage extends GridTcpCommunicationMessageAdapter {
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
    private Collection<IgniteEvent> evts;

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
    private IgniteDeploymentMode depMode;

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
        IgniteDeploymentMode depMode,
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
    GridEventStorageMessage(Collection<IgniteEvent> evts, Throwable ex) {
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
    @Nullable Collection<IgniteEvent> events() {
        return evts != null ? Collections.unmodifiableCollection(evts) : null;
    }

    /**
     * @param evts Events.
     */
    void events(@Nullable Collection<IgniteEvent> evts) {
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
    IgniteDeploymentMode deploymentMode() {
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
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridEventStorageMessage _clone = new GridEventStorageMessage();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
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
        commState.setBuffer(buf);

        if (!commState.typeWritten) {
            if (!commState.putByte(null, directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putGridUuid("clsLdrId", clsLdrId))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putEnum("depMode", depMode))
                    return false;

                commState.idx++;

            case 2:
                if (!commState.putByteArray("evtsBytes", evtsBytes))
                    return false;

                commState.idx++;

            case 3:
                if (!commState.putByteArray("exBytes", exBytes))
                    return false;

                commState.idx++;

            case 4:
                if (!commState.putByteArray("filter", filter))
                    return false;

                commState.idx++;

            case 5:
                if (!commState.putString("filterClsName", filterClsName))
                    return false;

                commState.idx++;

            case 6:
                if (ldrParties != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(null, ldrParties.size()))
                            return false;

                        commState.it = ldrParties.entrySet().iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        Map.Entry<UUID, IgniteUuid> e = (Map.Entry<UUID, IgniteUuid>)commState.cur;

                        if (!commState.keyDone) {
                            if (!commState.putUuid(null, e.getKey()))
                                return false;

                            commState.keyDone = true;
                        }

                        if (!commState.putGridUuid(null, e.getValue()))
                            return false;

                        commState.keyDone = false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(null, -1))
                        return false;
                }

                commState.idx++;

            case 7:
                if (!commState.putByteArray("resTopicBytes", resTopicBytes))
                    return false;

                commState.idx++;

            case 8:
                if (!commState.putString("userVer", userVer))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        switch (commState.idx) {
            case 0:
                clsLdrId = commState.getGridUuid("clsLdrId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 1:
                byte depMode0 = commState.getByte("depMode");

                if (!commState.lastRead())
                    return false;

                depMode = IgniteDeploymentMode.fromOrdinal(depMode0);

                commState.idx++;

            case 2:
                evtsBytes = commState.getByteArray("evtsBytes");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 3:
                exBytes = commState.getByteArray("exBytes");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 4:
                filter = commState.getByteArray("filter");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 5:
                filterClsName = commState.getString("filterClsName");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 6:
                if (commState.readSize == -1) {
                    commState.readSize = commState.getInt(null);

                    if (!commState.lastRead())
                        return false;
                }

                if (commState.readSize >= 0) {
                    if (ldrParties == null)
                        ldrParties = new HashMap<>(commState.readSize, 1.0f);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        if (!commState.keyDone) {
                            UUID _val = commState.getUuid(null);

                            if (!commState.lastRead())
                                return false;

                            commState.cur = _val;
                            commState.keyDone = true;
                        }

                        IgniteUuid _val = commState.getGridUuid(null);

                        if (!commState.lastRead())
                            return false;

                        ldrParties.put((UUID)commState.cur, _val);

                        commState.keyDone = false;

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;
                commState.cur = null;

                commState.idx++;

            case 7:
                resTopicBytes = commState.getByteArray("resTopicBytes");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 8:
                userVer = commState.getString("userVer");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

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
