package org.apache.ignite.internal.processors.odbc.protocol;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.portable.BinaryReaderExImpl;
import org.apache.ignite.internal.processors.odbc.GridOdbcRequest;
import org.apache.ignite.internal.processors.odbc.GridOdbcResponse;
import org.apache.ignite.internal.processors.odbc.handlers.GridOdbcQueryResult;
import org.apache.ignite.internal.util.nio.GridNioParser;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * ODBC protocol parser.
 */
public class GridOdbcParser implements GridNioParser {

    /** Length in bytes of the remaining message part. */
    int leftToReceive = 0;

    /** Already received bytes of current message. */
    ByteBuffer currentMessage = null;

    /** Context. */
    protected final GridKernalContext ctx;

    GridOdbcParser(GridKernalContext context) {
        ctx = context;
    }

    private byte[] tryConstructMessage(ByteBuffer buf) {
        if (leftToReceive != 0) {
            // Still receiving message
            int toConsume = Math.min(leftToReceive, buf.remaining());

            currentMessage.put(buf.array(), buf.arrayOffset(), toConsume);
            leftToReceive -= toConsume;

            buf.position(buf.position() + toConsume);

            if (leftToReceive != 0)
                return null;

            byte[] result = new byte[currentMessage.capacity()];

            currentMessage.get(result);
            currentMessage = null;

            return result;
        }

        // Receiving new message
        // Getting message length. It's in the first four bytes of the message.
        int messageLen = buf.getInt();
        int remaining = buf.remaining();

        if (messageLen > remaining) {
            leftToReceive = messageLen - remaining;

            currentMessage = ByteBuffer.allocate(messageLen);
            currentMessage.put(buf);

            return null;
        }

        byte[] result = new byte[messageLen];
        buf.get(result, 0, messageLen);

        return result;
    }

    @Nullable @Override public GridOdbcRequest decode(GridNioSession ses, ByteBuffer buf) throws IOException,
            IgniteCheckedException {
        byte[] message = tryConstructMessage(buf);

        return message == null ? null : parseMessage(message);
    }

    @Override public ByteBuffer encode(GridNioSession ses, Object msg0) throws IOException, IgniteCheckedException {
        assert msg0 != null;

        GridOdbcResponse msg = (GridOdbcResponse)msg0;

        //TODO: implement error encoding.
        if (msg.getSuccessStatus() != GridOdbcResponse.STATUS_SUCCESS) {
            ses.close();

            return null;
        }

        Object result0 = msg.getResponse();

        assert result0 instanceof GridOdbcQueryResult;

        GridOdbcQueryResult result = (GridOdbcQueryResult) result0;

        ByteBuffer response = ByteBuffer.allocate(8 + 4);

        response.putInt(8);
        response.putLong(result.getQueryId());

        return response;
    }

    private GridOdbcRequest parseMessage(byte[] msg) {
        BinaryReaderExImpl reader = new BinaryReaderExImpl(null, msg, 0, null);

        boolean local = reader.readBoolean();
        String sql = reader.readString();
        int pageSize = reader.readInt();
        int argsNum = reader.readInt();

        System.out.println("Message:");
        System.out.println("local: " + local);
        System.out.println("query: " + sql);
        System.out.println("pageSize: " + pageSize);
        System.out.println("argsNum: " + argsNum);

        return new GridOdbcRequest(sql, pageSize);
    }
}
