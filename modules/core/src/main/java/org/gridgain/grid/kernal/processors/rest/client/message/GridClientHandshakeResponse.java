/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.client.message;

import java.io.*;

/**
 * A client handshake response, containing result
 * code.
 */
public class GridClientHandshakeResponse extends GridClientAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final byte CODE_OK = 0;

    /** Response, indicating successful handshake. */
    public static final GridClientHandshakeResponse OK = new GridClientHandshakeResponse(CODE_OK);

    /** */
    private byte resCode;

    /**
     * Constructor for {@link Externalizable}.
     */
    public GridClientHandshakeResponse() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param resCode Result code.
     */
    public GridClientHandshakeResponse(byte resCode) {
        this.resCode = resCode;
    }

    /**
     * @return Result code.
     */
    public byte resultCode() {
        return resCode;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass().getSimpleName() + " [resCode=" + resCode + ']';
    }
}
