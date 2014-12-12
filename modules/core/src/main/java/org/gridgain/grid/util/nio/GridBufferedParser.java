/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.grid.util.nio;

import org.apache.ignite.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.nio.*;

/**
 * This class implements stream parser based on {@link GridNioServerBuffer}.
 * <p>
 * The rule for this parser is that every message sent over the stream is prepended with
 * 4-byte integer header containing message size. So, the stream structure is as follows:
 * <pre>
 *     +--+--+--+--+--+--+...+--+--+--+--+--+--+--+...+--+
 *     | MSG_SIZE  |   MESSAGE  | MSG_SIZE  |   MESSAGE  |
 *     +--+--+--+--+--+--+...+--+--+--+--+--+--+--+...+--+
 * </pre>
 * <p>
 * It expects that first 4 bytes in stream are {@link U#GG_HEADER}. If beginning of a stream,
 * isn't equal to these bytes than exception will be thrown.
 */
public class GridBufferedParser implements GridNioParser {
    /** Buffer metadata key. */
    private static final int BUF_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** */
    private final boolean directBuf;

    /** */
    private final ByteOrder order; // TODO: GG-6460

    /**
     * @param directBuf Direct buffer.
     * @param order Byte order.
     */
    public GridBufferedParser(boolean directBuf, ByteOrder order) {
        this.directBuf = directBuf;
        this.order = order;
    }

    /** {@inheritDoc} */
    @Override public byte[] decode(GridNioSession ses, ByteBuffer buf) throws IOException, IgniteCheckedException {
        GridNioServerBuffer nioBuf = ses.meta(BUF_META_KEY);

        // Decode for a given session is called per one thread, so there should not be any concurrency issues.
        // However, we make some additional checks.
        if (nioBuf == null) {
            nioBuf = new GridNioServerBuffer();

            GridNioServerBuffer old = ses.addMeta(BUF_META_KEY, nioBuf);

            assert old == null;
        }

        return nioBuf.read(buf);
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer encode(GridNioSession ses, Object msg) throws IOException, IgniteCheckedException {
        byte[] msg0 = (byte[])msg;

        ByteBuffer res = directBuf ? ByteBuffer.allocateDirect(msg0.length + 4) : ByteBuffer.allocate(msg0.length + 4);

        //res.order(order); // TODO: GG-6460

        res.putInt(msg0.length);
        res.put(msg0);

        res.flip();

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridBufferedParser.class.getSimpleName();
    }
}
