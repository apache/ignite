package org.gridgain.grid.kernal.processors.rest.protocols.tcp;

/**
 * Type of message being parsed.
 */
public enum GridClientPacketType {
    /** Memcache protocol message. */
    MEMCACHE,

    /** GridGain handshake. */
    GRIDGAIN_HANDSHAKE,

    /** GridGain message. */
    GRIDGAIN
}
