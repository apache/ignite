/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.impl.connection;

/**
 * Set of reasons why connection closed.
 */
enum GridClientConnectionCloseReason {
    /** Connection failed, IO exception or other unexpected result of request execution. */
    FAILED,

    /** Connection closed as idle. */
    CONN_IDLE,

    /** Client is closed and connection also shouldn't be used for new requests. */
    CLIENT_CLOSED
}
