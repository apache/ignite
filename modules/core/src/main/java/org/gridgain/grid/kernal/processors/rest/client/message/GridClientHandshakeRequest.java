/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.client.message;

import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.kernal.GridProductImpl.*;

/**
 * A client handshake request, containing version info and
 * a marshaller protocol ID.
 *
 * A handshake request structure is as follows:
 * <ol>
 *     <li>Message header (1 byte)</li>
 *     <li>Version info (4 bytes)</li>
 *     <li>Marshaller PROTOCOL_ID (1 byte)</li>
 * </ol>
 */
public class GridClientHandshakeRequest extends GridClientAbstractMessage {
    /** Packet size. */
    private static final int PACKET_SIZE = 6;

    /** Signal char. */
    public static final byte SIGNAL_CHAR = (byte)0x91;
    /** */
    private static final long serialVersionUID = 0L;


    /** Version info byte array. */
    private byte[] verArr;

    /** Protocol ID. */
    private byte protoId;

    /**
     * Default constructor.
     */
    public GridClientHandshakeRequest() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param protoId A protocol ID to use.
     */
    public GridClientHandshakeRequest(byte protoId) {
        this.protoId = protoId;
    }

    /**
     * @return Copy of version bytes or {@code null}, if version
     *         bytes were not set.
     */
    @Nullable public byte[] versionBytes() {
        if (verArr == null)
            return null;

        return Arrays.copyOf(verArr, verArr.length);
    }

    /**
     * Sets the version byte to a given value.
     *
     * @param idx Byte index.
     * @param b Byte value.
     */
    public void putVersionByte(int idx, byte b) {
        assert idx < VER_BYTES.length;

        if (verArr == null)
            verArr = new byte[VER_BYTES.length];

        verArr[idx] = b;
    }

    /**
     * Sets the version bytes from specified buffer to a given value.
     *
     * @param buf Buffer.
     * @param off Offset.
     * @param len Length.
     */
    public void putVersionBytes(byte[] buf, int off, int len) {
        if (verArr == null)
            verArr = new byte[VER_BYTES.length];

        U.arrayCopy(buf, 0, verArr, off, len);
    }

    /**
     * @param protoId New protocol ID.
     */
    public void protocolId(byte protoId) {
        this.protoId = protoId;
    }

    /**
     * @return Protocol ID.
     */
    public byte protocolId() {
        return protoId;
    }

    /**
     * @return Raw representation of this packet.
     */
    public byte[] rawBytes() {
        byte[] ret = new byte[PACKET_SIZE];

        ret[0] = SIGNAL_CHAR;
        ret[1] = VER_BYTES[0];
        ret[2] = VER_BYTES[1];
        ret[3] = VER_BYTES[2];
        ret[4] = VER_BYTES[3];
        ret[5] = protoId;

        return ret;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        U.writeByteArray(out, verArr);

        out.writeByte(protoId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        verArr = U.readByteArray(in);

        protoId = in.readByte();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass().getSimpleName() + " [verArr=" + Arrays.toString(verArr) +
            ", protoId=" + protoId + ']';
    }
}
