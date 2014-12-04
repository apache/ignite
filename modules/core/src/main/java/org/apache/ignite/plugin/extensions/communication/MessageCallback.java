/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.plugin.extensions.communication;

import org.apache.ignite.plugin.*;

import java.nio.*;
import java.util.*;

/**
 * Allows to patch message before sending or after reading.
 */
public interface MessageCallback extends IgniteExtension {
    /**
     * Writes delta for provided node and message type.
     *
     * @param nodeId Node ID.
     * @param msg Message type.
     * @param buf Buffer to write to.
     * @return Whether delta was fully written.
     */
    public boolean onSend(UUID nodeId, Object msg, ByteBuffer buf);

    /**
     * Reads delta for provided node and message type.
     *
     * @param nodeId Node ID.
     * @param msgCls Message type.
     * @param buf Buffer to read from.
     * @return Whether delta was fully read.
     */
    public boolean onReceive(UUID nodeId, Class<?> msgCls, ByteBuffer buf);
}
