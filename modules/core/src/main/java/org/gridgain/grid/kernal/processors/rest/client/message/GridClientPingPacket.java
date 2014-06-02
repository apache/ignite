/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.client.message;

/**
 * Fictive ping packet.
 */
public class GridClientPingPacket extends GridClientAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Ping message. */
    public static final GridClientMessage PING_MESSAGE = new GridClientPingPacket();

    /** Ping packet. */
    public static final byte[] PING_PACKET = new byte[] {(byte)0x90, 0x00, 0x00, 0x00, 0x00};

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass().getName();
    }
}
