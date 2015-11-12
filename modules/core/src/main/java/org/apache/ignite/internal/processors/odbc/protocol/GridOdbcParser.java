package org.apache.ignite.internal.processors.odbc.protocol;

import org.apache.ignite.IgniteCheckedException;
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

    @Nullable @Override public byte[] decode(GridNioSession ses, ByteBuffer buf) throws IOException,
            IgniteCheckedException {
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

    @Override public ByteBuffer encode(GridNioSession ses, Object msg) throws IOException, IgniteCheckedException {
        assert msg != null;

        byte[] messageBytes = (byte[])msg;

        ByteBuffer result = ByteBuffer.allocate(messageBytes.length + 4);

        result.putInt(messageBytes.length);
        result.put(messageBytes);

        return result;
    }
}
