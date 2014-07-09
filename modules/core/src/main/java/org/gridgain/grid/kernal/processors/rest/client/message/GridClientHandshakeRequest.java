/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.client.message;

import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * A client handshake request, containing version info and
 * a marshaller ID.
 *
 * A handshake request structure is as follows:
 * <ol>
 *     <li>Protocol version (2 bytes)</li>
 *     <li>Marshaller ID (2 bits)</li>
 *     <li>Reserved space (6 bits + 1 byte)</li>
 * </ol>
 */
public class GridClientHandshakeRequest extends GridClientAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Packet size. */
    private static final int PACKET_SIZE = 4;

    /** Protocol version. */
    private static final short PROTO_VER = 1;

    /** Handshake byte array. */
    private byte[] arr;

    /** Marshaller ID. */
    private byte marshId;

    /**
     * @return Protocol version.
     */
    public short version() {
        return U.bytesToShort(arr, 0);
    }

    /**
     * @return Marshaller ID.
     */
    public byte marshallerId() {
        return (byte)((arr[2] & 0xff) >> 6);
    }

    /**
     * @param marshId Marshaller ID.
     */
    public void marshallerId(byte marshId) {
        assert marshId >= 0 && marshId <= 2;

        this.marshId = marshId;
    }

    /**
     * Sets bytes from specified buffer to a given value.
     *
     * @param buf Buffer.
     * @param off Offset.
     * @param len Length.
     */
    public void putBytes(byte[] buf, int off, int len) {
        if (arr == null)
            arr = new byte[PACKET_SIZE];

        U.arrayCopy(buf, 0, arr, off, len);
    }

    /**
     * @return Raw representation of this packet.
     */
    public byte[] rawBytes() {
        byte[] ret = new byte[PACKET_SIZE];

        U.shortToBytes(PROTO_VER, ret, 0);

        ret[2] = (byte)(marshId << 6);

        return ret;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass().getSimpleName() + " [arr=" + Arrays.toString(arr) + ']';
    }
}
