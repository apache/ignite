// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nio;

import org.gridgain.grid.util.direct.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Message writer.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridNioMessageWriter {
    /**
     * @param nodeId Node ID.
     * @param msg Message to write.
     * @param buf Buffer.
     * @return Whether message was fully written.
     */
    public boolean write(@Nullable UUID nodeId, GridTcpCommunicationMessageAdapter msg, ByteBuffer buf);

    /**
     * @param nodeId Node ID.
     * @param msg Message to write.
     * @param out Output stream to write to.
     * @param buf Buffer.
     * @return Number of bytes written.
     * @throws IOException In case of error.
     */
    public int writeFully(@Nullable UUID nodeId, GridTcpCommunicationMessageAdapter msg, OutputStream out,
        ByteBuffer buf) throws IOException;
}
