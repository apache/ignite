/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.dr.messages.external;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.dr.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class GridDrExternalBatchRequest<K, V> implements Externalizable {
    /** Request ID. */
    private GridUuid reqId;

    /** Cache name. */
    private String cacheName;

    /** DR code. */
    private byte dataCenterId;

    /** Amount of entries. */
    private int entryCnt;

    /** Actual data to be replicated. */
    private Collection<GridDrRawEntry<K, V>> data;

    /** Marshalled data. */
    private byte[] dataBytes;

    /**
     * @param reqId Request ID.
     * @param cacheName Cache name
     * @param dataCenterId Data center ID.
     * @param entryCnt Amount of entries.
     * @param dataBytes Data bytes.
     */
    public GridDrExternalBatchRequest(GridUuid reqId, String cacheName, byte dataCenterId, int entryCnt,
        byte[] dataBytes) {
        assert reqId != null;
        assert entryCnt > 0;
        assert dataBytes != null && dataBytes.length > 0;

        this.reqId = reqId;
        this.cacheName = cacheName;
        this.dataCenterId = dataCenterId;
        this.entryCnt = entryCnt;
        this.dataBytes = dataBytes;
    }

    /**
     * {@link Externalizable} support.
     */
    public GridDrExternalBatchRequest() {
        // No-op.
    }

    /**
     * @return Request ID.
     */
    public GridUuid requestId() {
        return reqId;
    }

    /**
     * @return Cache name.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return DR code.
     */
    public byte dataCenterId() {
        return dataCenterId;
    }

    /**
     * @return Amount of entries.
     */
    public int entryCount() {
        return entryCnt;
    }

    /**
     * @return Amount of bytes.
     */
    public int dataSize() {
        return dataBytes.length;
    }
    /**
     * @return Data.
     */
    public Collection<GridDrRawEntry<K, V>> data() {
        assert data != null;

        return data;
    }
    /**
     * @param marsh Marshaller.
     * @throws GridException If failed.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    public void prepare(GridMarshaller marsh) throws GridException {
        assert data == null;

        data = new ArrayList<>(entryCnt);

        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(dataBytes))) {
            for (int i = 0; i < entryCnt; i++)
                data.add(GridDrUtils.<K, V>readDrEntry(in, dataCenterId));
        }
        catch (IOException e) {
            throw new GridException("Failed to unmarshal external data center replication batch request.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, reqId);
        U.writeString(out, cacheName);
        out.writeByte(dataCenterId);
        out.writeInt(entryCnt);
        U.writeByteArray(out, dataBytes);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        reqId = U.readGridUuid(in);
        cacheName = U.readString(in);
        dataCenterId = in.readByte();
        entryCnt = in.readInt();
        dataBytes = U.readByteArray(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDrExternalBatchRequest.class, this, "size",
            dataBytes != null ? dataBytes.length : "N/A");
    }
}
