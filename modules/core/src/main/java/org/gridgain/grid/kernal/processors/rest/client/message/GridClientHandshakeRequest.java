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

import java.util.*;

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
    /** */
    private static final long serialVersionUID = 0L;

    /** Packet size. */
    private static final int PACKET_SIZE = 5;

    /** Protocol version. */
    private static final short PROTO_VER = 1;

    /** Signal char. */
    public static final byte SIGNAL_CHAR = (byte)0x91;

    /** Version info byte array. */
    private byte[] verArr;

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
     * Sets the version bytes from specified buffer to a given value.
     *
     * @param buf Buffer.
     * @param off Offset.
     * @param len Length.
     */
    public void putVersionBytes(byte[] buf, int off, int len) {
        if (verArr == null)
            verArr = new byte[PACKET_SIZE - 1];

        U.arrayCopy(buf, 0, verArr, off, len);
    }

    /**
     * @return Raw representation of this packet.
     */
    public byte[] rawBytes() {
        byte[] ret = new byte[PACKET_SIZE];

        ret[0] = SIGNAL_CHAR;

        U.shortToBytes(PROTO_VER, ret, 1);

        return ret;
    }

    /**
     * @return Raw representation of this packet.
     */
    public byte[] rawBytesNoHeader() {
        byte[] ret = new byte[PACKET_SIZE - 1];

        U.shortToBytes(PROTO_VER, ret, 0);

        return ret;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass().getSimpleName() + " [verArr=" + Arrays.toString(verArr) + ']';
    }
}
