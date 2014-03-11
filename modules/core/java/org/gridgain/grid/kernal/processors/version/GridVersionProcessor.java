/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.version;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.product.*;

import java.nio.*;
import java.util.*;

/**
 * Version converter processor.
 */
public interface GridVersionProcessor extends GridProcessor {
    /**
     * Handles node start.
     *
     * @param remoteNodes Remote grid nodes.
     */
    public void onStart(Collection<GridNode> remoteNodes);

    /**
     * Handles node joined event.
     *
     * @param node Joined node.
     */
    public void onNodeJoined(GridNode node);

    /**
     * Handles node left event.
     *
     * @param node Left node.
     */
    public void onNodeLeft(GridNode node);

    /**
     * Writes delta for provided node and message type.
     *
     * @param nodeId Node ID.
     * @param msgCls Message type.
     * @param buf Buffer to write to.
     * @return Whether delta was fully written.
     */
    public boolean writeDelta(UUID nodeId, Class<?> msgCls, ByteBuffer buf);

    /**
     * Reads delta for provided node and message type.
     *
     * @param nodeId Node ID.
     * @param msgCls Message type.
     * @param buf Buffer to read from.
     * @return Whether delta was fully read.
     */
    public boolean readDelta(UUID nodeId, Class<?> msgCls, ByteBuffer buf);

    /**
     * Locally registers converter for a class.
     *
     * @param cls Class.
     * @param converterCls Converter class.
     * @param fromVer Version to convert from.
     * @throws GridException In case of error.
     */
    public void registerLocal(Class<?> cls, Class<? extends GridVersionConverter> converterCls,
        GridProductVersion fromVer) throws GridException;

    /**
     * Adds all registered local converters to node attributes.
     *
     * @param attrs Node attributes.
     */
    public void addConvertersToAttributes(Map<String, Object> attrs);
}
