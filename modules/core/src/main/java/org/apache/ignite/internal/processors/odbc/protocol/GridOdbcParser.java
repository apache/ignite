package org.apache.ignite.internal.processors.odbc.protocol;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.portable.BinaryReaderExImpl;
import org.apache.ignite.internal.portable.BinaryWriterExImpl;
import org.apache.ignite.internal.processors.odbc.request.GridOdbcRequest;
import org.apache.ignite.internal.processors.odbc.GridOdbcResponse;
import org.apache.ignite.internal.processors.odbc.handlers.GridOdbcQueryResult;
import org.apache.ignite.internal.processors.odbc.request.QueryCloseRequest;
import org.apache.ignite.internal.processors.odbc.request.QueryExecuteRequest;
import org.apache.ignite.internal.processors.odbc.request.QueryFetchRequest;
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

        return message == null ? null : parseMessage(ses, message);
    }

    @Override public ByteBuffer encode(GridNioSession ses, Object msg0) throws IOException, IgniteCheckedException {
        assert msg0 != null;
        assert msg0 instanceof GridOdbcResponse;

        System.out.println("Encoding query processing result");

        GridOdbcResponse msg = (GridOdbcResponse)msg0;

        //TODO: implement error encoding.
        if (msg.getSuccessStatus() != GridOdbcResponse.STATUS_SUCCESS) {

            System.out.println("Error: " + msg.getError());

            ses.close();

            return null;
        }

        Object result0 = msg.getResponse();

        assert result0 instanceof GridOdbcQueryResult;

        GridOdbcQueryResult result = (GridOdbcQueryResult) result0;

        System.out.println("Resulting query ID: " + result.getQueryId());

        ByteBuffer buf = ByteBuffer.allocate(8 + 4);

        BinaryWriterExImpl writer = new BinaryWriterExImpl(null, 0, false);

        buf.putInt(8);
        buf.putLong(result.getQueryId());

        System.out.println("Remaining: " + buf.remaining());

        buf.flip();

        return buf;
    }

    private GridOdbcRequest parseMessage(GridNioSession ses, byte[] msg) throws IOException {
        BinaryReaderExImpl reader = new BinaryReaderExImpl(null, msg, 0, null);

        GridOdbcRequest res;

        byte cmd = reader.readByte();

        switch (cmd) {
            case GridOdbcRequest.EXECUTE_SQL_QUERY: {
                String cache = reader.readString();
                String sql = reader.readString();
                int argsNum = reader.readInt();

                System.out.println("Message EXECUTE_SQL_QUERY:");
                System.out.println("cache: " + cache);
                System.out.println("query: " + sql);
                System.out.println("argsNum: " + argsNum);

                res = new QueryExecuteRequest(cache, sql);
                break;
            }

            case GridOdbcRequest.FETCH_SQL_QUERY: {
                long queryId = reader.readLong();
                int pageSize = reader.readInt();

                System.out.println("Message FETCH_SQL_QUERY:");
                System.out.println("queryId: " + queryId);
                System.out.println("pageSize: " + pageSize);

                res = new QueryFetchRequest(queryId, pageSize);
                break;
            }

            case GridOdbcRequest.CLOSE_SQL_QUERY: {
                long queryId = reader.readLong();

                System.out.println("Message CLOSE_SQL_QUERY:");
                System.out.println("queryId: " + queryId);

                res = new QueryCloseRequest(queryId);
                break;
            }

            default:
                throw new IOException("Failed to parse incoming packet (unknown command type) [ses=" + ses +
                        ", cmd=[" + Byte.toString(cmd) + ']');
        }

        return res;
    }
}
