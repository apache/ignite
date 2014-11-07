/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.dr;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.dr.messages.external.*;
import org.gridgain.grid.util.io.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * DR utility methods.
 */
public class GridDrUtils {
    /** Maximum amount of data centers. */
    public static final int MAX_DATA_CENTERS = 32;

    /** External message type: handshake request. */
    private static final byte TYP_HND_REQ = 0;

    /** External message type: handshake response. */
    private static final byte TYP_HND_RESP = 1;

    /** External message type: ping request. */
    private static final byte TYP_PING_REQ = 2;

    /** External message type: ping response. */
    private static final byte TYP_PING_RESP = 3;

    /** External message type: batch request. */
    private static final byte TYP_BATCH_REQ = 4;

    /** External message type: batch response. */
    private static final byte TYP_BATCH_RESP = 5;

    /** Cached external ping request. */
    private static final GridDrExternalPingRequest PING_REQ = new GridDrExternalPingRequest();

    /** Cached external ping response. */
    private static final GridDrExternalPingResponse PING_RESP = new GridDrExternalPingResponse();

    /** Cached external ping request bytes. */
    public static final byte[] PING_REQ_BYTES;

    /** Cached external ping response bytes. */
    public  static final byte[] PING_RESP_BYTES;

    /**
     *
     */
    static {
        PING_REQ_BYTES = new byte[5];

        U.intToBytes(1, PING_REQ_BYTES, 0);

        PING_REQ_BYTES[4] = TYP_PING_REQ;

        PING_RESP_BYTES = new byte[] { TYP_PING_RESP };
    }

    /**
     * Read marshaled DR entry.
     *
     * @param in Input.
     * @param dataCenterId Data center ID.
     * @return Marshalled DR entry.
     * @throws IOException If failed.
     */
    public static <K, V> GridDrRawEntry<K, V> readDrEntry(DataInput in, byte dataCenterId) throws IOException {
        byte[] keyBytes = U.readByteArray(in);
        byte[] valBytes = U.readByteArray(in);

        long ttl;
        long expireTime;

        if (in.readBoolean()) {
            ttl = in.readLong();
            expireTime = in.readLong();
        }
        else {
            ttl = 0L;
            expireTime = 0L;
        }

        GridCacheVersion ver = new GridCacheVersion(in.readInt(), in.readLong(), in.readLong(), in.readInt(),
            dataCenterId);

        return new GridDrRawEntry<>(null, keyBytes, null, valBytes, ttl, expireTime, ver);
    }

    /**
     * Write marshalled DR entry.
     *
     * @param out Output.
     * @param entry Entry.
     * @throws IOException If failed.
     */
    public static <K, V> void writeDrEntry(DataOutput out, GridDrRawEntry<K, V> entry) throws IOException {
        assert entry.keyBytes() != null;

        // Write key and value.
        U.writeByteArray(out, entry.keyBytes());
        U.writeByteArray(out, entry.valueBytes());

        // Writ expiration info.
        if (entry.ttl() > 0) {
            out.writeBoolean(true);
            out.writeLong(entry.ttl());
            out.writeLong(entry.expireTime());
        }
        else
            out.writeBoolean(false);

        // Write version.
        out.writeInt(entry.version().topologyVersion());
        out.writeLong(entry.version().globalTime());
        out.writeLong(entry.version().order());
        out.writeInt(entry.version().nodeOrder());
    }

    /**
     * Marshal handshake request.
     *
     * @param req Handshake request.
     * @return Bytes.
     * @throws GridException If failed.
     */
    public static byte[] marshal(GridDrExternalHandshakeRequest req) throws GridException {
        // 17 = 4 (size) + 1 (type) + 1 (data center ID) + 1 (proto string flag) + 4 (proto string len) +
        //      1 (marsh class string flag) + 4 (marsh class string len) + 1 (ack flag).
        int size = 17 + U.GG_HEADER.length + req.protocolVersion().length() + req.marshallerClassName().length();

        GridUnsafeDataOutput out = new GridUnsafeDataOutput(size);

        try {
            // Special handling for GG header.
            System.arraycopy(U.GG_HEADER, 0, out.internalArray(), 0, U.GG_HEADER.length);
            out.offset(U.GG_HEADER.length);

            writeSize(out, size - 4 - U.GG_HEADER.length);
            out.writeByte(TYP_HND_REQ);
            out.writeByte(req.dataCenterId());
            U.writeString(out, req.protocolVersion());
            U.writeString(out, req.marshallerClassName());
            out.writeBoolean(req.awaitAcknowledge());

            // Do not copy array in case we guessed size correctly.
            return out.offset() == size ? out.internalArray() : out.array();
        }
        catch (IOException e) {
            throw new GridException("Failed to marshal DR handshake request.", e);
        }
    }

    /**
     * Marshal handshake response.
     *
     * @param resp Handshake response.
     * @return Bytes.
     * @throws GridException If failed.
     */
    public static byte[] marshal(GridDrExternalHandshakeResponse resp) throws GridException {
        // 2 = 1 (type) + 1 (error message flag).
        int size = 2;

        String errMsg = resp.errorMessage();

        if (errMsg != null)
            size += 4 + errMsg.length();

        GridUnsafeDataOutput out = new GridUnsafeDataOutput(size);

        try {
            out.writeByte(TYP_HND_RESP);
            U.writeString(out, resp.errorMessage());

            // Do not copy array in case we guessed size correctly.
            return out.offset() == size ? out.internalArray() : out.array();
        }
        catch (IOException e) {
            throw new GridException("Failed to marshal DR handshake response.", e);
        }
    }

    /**
     * Marshal batch request.
     *
     * @param req Batch request.
     * @return Bytes.
     * @throws GridException If failed.
     */
    public static byte[] marshal(GridDrExternalBatchRequest req) throws GridException {
        assert req.dataBytes() != null : "DR batch request is not prepared: " + req;

        // 44 = 4 (size) + 1 (type) + 1 (cache name string flag) + 4 (entry cnt) + 4 (bytes cnt) + 1 (data center ID) +
        //      25 (request ID) + 4 (data byte array length).
        int size = 44 + req.dataBytes().length;

        String cacheName = req.cacheName();

        if (cacheName != null)
            size += 4 + cacheName.length();

        GridUnsafeDataOutput out = new GridUnsafeDataOutput(size);

        try {
            writeSize(out, size - 4);
            out.writeByte(TYP_BATCH_REQ);
            U.writeGridUuid(out, req.requestId());
            U.writeString(out, cacheName);
            out.writeInt(req.entryCount());
            out.writeInt(req.dataBytes().length);
            out.writeByte(req.dataCenterId());
            U.writeByteArray(out, req.dataBytes());

            // Do not copy array in case we guessed size correctly.
            return out.offset() == size ? out.internalArray() : out.array();
        }
        catch (IOException e) {
            throw new GridException("Failed to marshal DR batch request.", e);
        }
    }

    /**
     * Marshal batch response.
     *
     * @param resp Batch response.
     * @return Bytes.
     * @throws GridException If failed.
     */
    public static byte[] marshal(GridDrExternalBatchResponse resp) throws GridException {
        // 27 = 1 (type) + 25 (GridUuid) + 1 (error message flag).
        int size = 27;

        String errMsg = resp.errorMessage();

        if (errMsg != null)
            size += 4 + errMsg.length();

        GridUnsafeDataOutput out = new GridUnsafeDataOutput(size);

        try {
            out.writeByte(TYP_BATCH_RESP);
            U.writeGridUuid(out, resp.requestId());
            U.writeString(out, resp.errorMessage());

            // Do not copy array in case we guessed size correctly.
            return out.offset() == size ? out.internalArray() : out.array();
        }
        catch (IOException e) {
            throw new GridException("Failed to marshal DR batch response.", e);
        }
    }

    /**
     * Write request size.
     *
     * @param out Output.
     * @param size Size.
     */
    private static void writeSize(GridUnsafeDataOutput out, int size) {
        U.intToBytes(size, out.internalArray(), out.offset());

        out.offset(out.offset() + 4);
    }

    /**
     * Unmarshal DR message.
     *
     * @param data Data bytes.
     * @return Message.
     * @throws GridException If failed.
     */
    public static Object unmarshal(byte[] data) throws GridException {
        assert data != null;

        GridUnsafeDataInput in = new GridUnsafeDataInput();

        in.bytes(data, data.length);

        try {
            byte typ = in.readByte();

            switch (typ) {
                case TYP_HND_REQ:
                    return new GridDrExternalHandshakeRequest(in.readByte(), U.readString(in), U.readString(in),
                        in.readBoolean());

                case TYP_HND_RESP:
                    return new GridDrExternalHandshakeResponse(U.readString(in));

                case TYP_PING_REQ:
                    return PING_REQ;

                case TYP_PING_RESP:
                    return PING_RESP;

                case TYP_BATCH_REQ:
                    GridUuid reqId = U.readGridUuid(in);
                    String cacheName = U.readString(in);
                    int entryCnt = in.readInt();
                    in.readInt(); // Skip bytes count as we do not need it here.

                    byte dataCenterId = in.readByte();
                    byte[] dataBytes = U.readByteArray(in);

                    return new GridDrExternalBatchRequest<>(reqId, cacheName, dataCenterId, entryCnt, dataBytes);

                case TYP_BATCH_RESP:
                    return new GridDrExternalBatchResponse(U.readGridUuid(in), U.readString(in));

                default:
                    throw new GridException("Unknown DR message type: " + typ);
            }
        }
        catch (IOException e) {
            throw new GridException("Failed to unmarshal DR message.", e);
        }
    }

    /**
     * Get batch request header.
     *
     * @param data Raw data bytes.
     * @return Batch request header.
     * @throws GridException If failed.
     */
    public static GridTuple4<GridUuid, String, Integer, Integer> batchRequestHeader(byte[] data) throws GridException {
        GridUnsafeDataInput in = new GridUnsafeDataInput();

        in.bytes(data, data.length);

        try {
            in.skipBytes(5); // Skip length and type.

            return F.t(U.readGridUuid(in), U.readString(in), in.readInt(), in.readInt());
        }
        catch (IOException e) {
            throw new GridException("Failed to read DR batch request header.", e);
        }
    }

    /**
     * Force singleton.
     */
    private GridDrUtils() {
        // No-op.
    }
}
