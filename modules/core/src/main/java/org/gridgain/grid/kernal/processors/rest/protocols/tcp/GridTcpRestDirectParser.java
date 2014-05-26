package org.gridgain.grid.kernal.processors.rest.protocols.tcp;

import org.gridgain.client.marshaller.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.rest.client.message.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.nio.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

import static org.gridgain.grid.util.nio.GridNioSessionMetaKey.*;

public class GridTcpRestDirectParser implements GridNioParser {
    /** Message metadata key. */
    private static final int MSG_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** Message reader. */
    private final GridNioMessageReader msgReader;

    /**
     * @param msgReader Message reader.
     */
    public GridTcpRestDirectParser(GridNioMessageReader msgReader) {
        this.msgReader = msgReader;
    }

    /** {@inheritDoc} */
    @Nullable
    @Override public Object decode(GridNioSession ses, ByteBuffer buf) throws IOException, GridException {
        GridTcpCommunicationMessageAdapter msg = ses.removeMeta(MSG_META_KEY);
        UUID nodeId = ses.meta(GridNioServer.DIFF_VER_NODE_ID_META_KEY);

        if (msg == null && buf.hasRemaining()) {
            byte type = buf.get();

            if (type == GridClientMessageWrapper.REQ_HEADER)
                msg = new GridClientMessageWrapper();
            else if (type == GridClientHandshakeRequestWrapper.SIGNAL_CHAR)
                msg = new GridClientHandshakeRequestWrapper();
        }

        boolean finished = false;

        if (buf.hasRemaining())
            finished = msgReader.read(nodeId, msg, buf);

        if (finished) {
            if (msg instanceof GridClientMessageWrapper) {
                GridClientMessageWrapper clientMsg = (GridClientMessageWrapper)msg;

                GridClientMarshaller marsh = marshaller(ses);

                GridClientMessage ret = marsh.unmarshal(clientMsg.getMsg());

                ret.requestId(clientMsg.getReqId());
                ret.clientId(clientMsg.getClientId());
                ret.destinationId(clientMsg.getDestId());

                return ret;
            }
            else {
                assert msg instanceof GridClientHandshakeRequestWrapper;

                GridClientHandshakeRequest ret = new GridClientHandshakeRequest();

                ret.protocolId(((GridClientHandshakeRequestWrapper)msg).protocol());

                return ret;
            }
        }
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

    /**
     * Returns marshaller from session, if no marshaller found - init it with default.
     *
     * @param ses Current session.
     * @return Current session's marshaller.
     */
    protected GridClientMarshaller marshaller(GridNioSession ses) {
        GridClientMarshaller marsh = ses.meta(MARSHALLER.ordinal());

        if (marsh == null) {
            //U.warn(log, "No marshaller defined for NIO session, using PROTOBUF as default [ses=" + ses + ']');

            //marsh = protobufMarshaller;

            assert false;

            ses.addMeta(MARSHALLER.ordinal(), marsh);
        }

        return marsh;
    }
}
