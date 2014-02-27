// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.dr.messages.internal;

import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Internal DR request.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridDrInternalRequest extends GridTcpCommunicationMessageAdapter {
    /** Request ID. */
    private long id;

    /** Cache name. */
    private String cacheName;

    /** Destination data center IDs (if any). */
    @GridDirectCollection(byte.class)
    private Collection<Byte> dataCenterIds;

    /** Entries. */
    @GridDirectCollection(GridDrInternalRequestEntry.class)
    private Collection<GridDrInternalRequestEntry> entries;

    /** Entry count (total). */
    private int entryCnt;

    /**
     * {@link Externalizable} support.
     */
    public GridDrInternalRequest() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param id Request ID.
     * @param cacheName Cache name.
     * @param dataCenterIds Destination data center IDs (if any).
     * @param entries Entries.
     * @param entryCnt Entry count.
     */
    public GridDrInternalRequest(long id, String cacheName, @Nullable Collection<Byte> dataCenterIds,
        Collection<GridDrInternalRequestEntry> entries, int entryCnt) {
        assert !F.isEmpty(entries);
        assert entryCnt > 0;

        this.id = id;
        this.cacheName = cacheName;
        this.dataCenterIds = dataCenterIds;
        this.entries = entries;
        this.entryCnt = entryCnt;
    }

    /**
     * @return Request id.
     */
    public long id() {
        return id;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Sequence.
     */
    public Collection<Byte> dataCenterIds() {
        return dataCenterIds;
    }

    /**
     * @return Tracked data center IDs.
     */
    public Collection<GridDrInternalRequestEntry> entries() {
        return entries;
    }

    /**
     * @return Amount of entries within marshaled data.
     */
    public int entryCount() {
        return entryCnt;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridDrInternalRequest _clone = new GridDrInternalRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridDrInternalRequest _clone = (GridDrInternalRequest)_msg;

        _clone.id = id;
        _clone.cacheName = cacheName;
        _clone.dataCenterIds = dataCenterIds;
        _clone.entries = entries;
        _clone.entryCnt = entryCnt;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putString(cacheName))
                    return false;

                commState.idx++;

            case 1:
                if (dataCenterIds != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(dataCenterIds.size()))
                            return false;

                        commState.it = dataCenterIds.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putByte((byte)commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(-1))
                        return false;
                }

                commState.idx++;

            case 2:
                if (entries != null) {
                    if (commState.it == null) {
                        if (!commState.putInt(entries.size()))
                            return false;

                        commState.it = entries.iterator();
                    }

                    while (commState.it.hasNext() || commState.cur != NULL) {
                        if (commState.cur == NULL)
                            commState.cur = commState.it.next();

                        if (!commState.putDrInternalRequestEntry((GridDrInternalRequestEntry)commState.cur))
                            return false;

                        commState.cur = NULL;
                    }

                    commState.it = null;
                } else {
                    if (!commState.putInt(-1))
                        return false;
                }

                commState.idx++;

            case 3:
                if (!commState.putInt(entryCnt))
                    return false;

                commState.idx++;

            case 4:
                if (!commState.putLong(id))
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
                String cacheName0 = commState.getString();

                if (cacheName0 == STR_NOT_READ)
                    return false;

                cacheName = cacheName0;

                commState.idx++;

            case 1:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (dataCenterIds == null)
                        dataCenterIds = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        if (buf.remaining() < 1)
                            return false;

                        byte _val = commState.getByte();

                        dataCenterIds.add((Byte)_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 2:
                if (commState.readSize == -1) {
                    if (buf.remaining() < 4)
                        return false;

                    commState.readSize = commState.getInt();
                }

                if (commState.readSize >= 0) {
                    if (entries == null)
                        entries = new ArrayList<>(commState.readSize);

                    for (int i = commState.readItems; i < commState.readSize; i++) {
                        GridDrInternalRequestEntry _val = commState.getDrInternalRequestEntry();

                        if (_val == DR_INT_REQ_ENTRY_NOT_READ)
                            return false;

                        entries.add((GridDrInternalRequestEntry)_val);

                        commState.readItems++;
                    }
                }

                commState.readSize = -1;
                commState.readItems = 0;

                commState.idx++;

            case 3:
                if (buf.remaining() < 4)
                    return false;

                entryCnt = commState.getInt();

                commState.idx++;

            case 4:
                if (buf.remaining() < 8)
                    return false;

                id = commState.getLong();

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 63;
    }
}
