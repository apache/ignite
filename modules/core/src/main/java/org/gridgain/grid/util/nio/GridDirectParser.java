/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nio;

import org.gridgain.grid.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.direct.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Parser for direct messages.
 */
public class GridDirectParser implements GridNioParser {
    /** Message metadata key. */
    private static final int MSG_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** Message reader. */
    private final GridNioMessageReader msgReader;

    /** */
    private IgniteSpiAdapter spi;

    /** */
    private GridTcpMessageFactory msgFactory;

    /**
     * @param msgReader Message reader.
     * @param spi Spi.
     */
    public GridDirectParser(GridNioMessageReader msgReader, IgniteSpiAdapter spi) {
        this.msgReader = msgReader;
        this.spi = spi;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object decode(GridNioSession ses, ByteBuffer buf) throws IOException, GridException {
        if (msgFactory == null)
            msgFactory = spi.getSpiContext().messageFactory();

        GridTcpCommunicationMessageAdapter msg = ses.removeMeta(MSG_META_KEY);
        UUID nodeId = ses.meta(GridNioServer.DIFF_VER_NODE_ID_META_KEY);

        if (msg == null && buf.hasRemaining())
            msg = msgFactory.create(buf.get());

        boolean finished = false;

        if (buf.hasRemaining())
            finished = msgReader.read(nodeId, msg, buf);

        if (finished)
            return msg;
        else {
            ses.addMeta(MSG_META_KEY, msg);

            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer encode(GridNioSession ses, Object msg) throws IOException, GridException {
        // No encoding needed for direct messages.
        throw new UnsupportedEncodingException();
    }
}
