/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

/**
 * This exception is thrown when a client handshake has failed.
 */
public class GridClientHandshakeException extends GridClientException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Status code for handshake error. */
    private final byte statusCode;

    /**
     * Constructor.
     *
     * @param statusCode Error status code.
     * @param msg Error message.
     */
    public GridClientHandshakeException(byte statusCode, String msg) {
        super(msg);

        this.statusCode = statusCode;
    }

    /**
     * @return Error status code.
     */
    public byte getStatusCode() {
        return statusCode;
    }
}
