/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nio;

import org.gridgain.grid.util.direct.*;
import org.jetbrains.annotations.*;

import java.nio.*;
import java.util.*;

/**
 * Message reader.
 */
public interface GridNioMessageReader {
    /**
     * @param nodeId Node ID.
     * @param msg Message to read.
     * @param buf Buffer.
     * @return Whether message was fully read.
     */
    public boolean read(@Nullable UUID nodeId, GridTcpCommunicationMessageAdapter msg, ByteBuffer buf);

    /**
     * @return Optional message factory.
     */
    @Nullable public GridTcpMessageFactory messageFactory();
}
