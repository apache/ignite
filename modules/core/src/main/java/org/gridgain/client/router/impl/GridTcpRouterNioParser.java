/* @java.file.header */

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
import org.gridgain.grid.util.nio.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.nio.*;

import static org.gridgain.grid.kernal.processors.rest.protocols.tcp.GridMemcachedMessage.*;

/**
 *
 */
class GridTcpRouterNioParser extends GridTcpRestParser {
    /** Number of received messages. */
    private volatile long rcvCnt;

    /** Number of sent messages. */
    private volatile long sndCnt;

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

            GridClientMessage clientMsg = (GridClientMessage)msg;

            ByteBuffer res = marsh.marshal(msg, 45);

            ByteBuffer slice = res.slice();

            slice.put(GRIDGAIN_REQ_FLAG);
            slice.putInt(res.remaining() - 5);
            slice.putLong(clientMsg.requestId());
            slice.put(U.uuidToBytes(clientMsg.clientId()));
            slice.put(U.uuidToBytes(clientMsg.destinationId()));

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
