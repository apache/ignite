// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.router.impl;

import org.gridgain.client.marshaller.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.rest.client.message.*;
import org.gridgain.grid.kernal.processors.rest.protocols.tcp.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.nio.*;

import java.io.*;
import java.nio.*;

import static org.gridgain.grid.kernal.processors.rest.protocols.tcp.GridMemcachedMessage.*;

/**
 * @author @java.author
 * @version @java.version
 */
class GridTcpRouterNioParser extends GridTcpRestParser {
    /** Number of received messages. */
    private volatile long rcvCnt;

    /** Number of sent messages. */
    private volatile long sndCnt;

    /**
     * @param log Logger.
     */
    GridTcpRouterNioParser(GridLogger log) {
        super(log);
    }

    /** {@inheritDoc} */
    @Override protected GridClientMessage parseClientMessage(GridNioSession ses, ParserState state) {
        rcvCnt++;

        return new GridRouterRequest(
            state.buffer().toByteArray(),
            state.header().reqId(),
            state.header().clientId(),
            state.header().destinationId());
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer encode(GridNioSession ses, Object msg) throws IOException, GridException {
        sndCnt++;

        if (msg instanceof GridRouterResponse) {
            GridRouterResponse resp = (GridRouterResponse)msg;

            ByteBuffer res = ByteBuffer.allocate(resp.body().length + 45);

            res.put(GRIDGAIN_REQ_FLAG);
            res.putInt(resp.body().length + 40);
            res.putLong(resp.requestId());
            res.put(U.uuidToBytes(resp.clientId()));
            res.put(U.uuidToBytes(resp.destinationId()));
            res.put(resp.body());

            res.flip();

            return res;
        }
        else if (msg instanceof GridClientResponse) {
            GridClientMarshaller marsh = marshaller(ses);

            byte[] msgBytes = marsh.marshal(msg);

            ByteBuffer res = ByteBuffer.allocate(msgBytes.length + 45);

            GridClientMessage clientMsg = (GridClientMessage) msg;

            res.put(GRIDGAIN_REQ_FLAG);
            res.put(U.intToBytes(msgBytes.length + 40));
            res.putLong(clientMsg.requestId());
            res.put(U.uuidToBytes(clientMsg.clientId()));
            res.put(U.uuidToBytes(clientMsg.destinationId()));

            res.put(msgBytes);

            res.flip();

            return res;
        }
        else if (msg instanceof GridClientPingPacket || msg instanceof GridClientHandshakeResponse) {
            return super.encode(ses, msg);
        }
        else
            throw new GridException("Unsupported message: " + msg);
    }

    /**
     * @return Number of received messages.
     */
    public long getReceivedCount() {
        return rcvCnt;
    }

    /**
     * @return Number of sent messages.
     */
    public long getSendCount() {
        return sndCnt;
    }
}
