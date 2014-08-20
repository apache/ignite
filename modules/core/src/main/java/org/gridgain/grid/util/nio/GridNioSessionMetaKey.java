/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nio;

import java.util.concurrent.atomic.*;

/**
 * Meta keys for {@link GridNioSession}.
 */
public enum GridNioSessionMetaKey {
    /** NIO parser state. */
    PARSER_STATE,

    /** SSL handler. */
    SSL_HANDLER,

    /** NIO operation (request type). */
    NIO_OPERATION,

    /** Last future. */
    LAST_FUT,

    /** Client marshaller. */
    MARSHALLER,

    /** Client marshaller ID. */
    MARSHALLER_ID;

    /** Maximum count of NIO session keys in system. */
    public static final int MAX_KEYS_CNT = 64;

    /** NIO session key generator. */
    private static final AtomicInteger keyGen = new AtomicInteger(GridNioSessionMetaKey.values().length);

    /**
     * Returns next NIO session key ordinal for non-existing enum value.
     * <p>
     * NOTE: Maximum count of NIO session keys in system is limited by {@link #MAX_KEYS_CNT}.
     *
     * @return NIO session key ordinal for non-existing enum value.
     */
    public static int nextUniqueKey() {
        int res = keyGen.getAndIncrement();

        if (res >= MAX_KEYS_CNT)
            throw new IllegalStateException("Maximum count of NIO session keys in system is limited by: " +
                MAX_KEYS_CNT);

        return res;
    }
}
