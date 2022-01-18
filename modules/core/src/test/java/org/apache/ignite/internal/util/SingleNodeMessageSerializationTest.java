package org.apache.ignite.internal.util;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.internal.direct.DirectMessageReader;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.plugin.extensions.communication.IgniteMessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.TEST_PROCESS;

/**
 * Single node message serialization test.
 */
public class SingleNodeMessageSerializationTest extends GridCommonAbstractTest {
    /** Protocol version. */
    private static final byte PROTO_VER = 2;

    /** Message factory. */
    private final IgniteMessageFactory msgFactory =
        new IgniteMessageFactoryImpl(new MessageFactory[] {new GridIoMessageFactory()});

    /** */
    @Test
    public void testDataMessage() {
        doMarshalUnmarshal(new SingleNodeMessage<>(UUID.randomUUID(), TEST_PROCESS, "data", null));
    }

    /** */
    @Test
    public void testErrorMessage() {
        doMarshalUnmarshal(new SingleNodeMessage<>(UUID.randomUUID(), TEST_PROCESS, null, new Exception()));
    }

    /** */
    @Test
    public void testEmptyMessage() {
        doMarshalUnmarshal(new SingleNodeMessage<>(UUID.randomUUID(), TEST_PROCESS, null, null));
    }

    /**
     * @param srcMsg Source message.
     */
    private void doMarshalUnmarshal(SingleNodeMessage<?> srcMsg) {
        ByteBuffer buf = ByteBuffer.allocate(8 * 1024);

        CI2<Integer, Function<ByteBuffer, Boolean>> bufLoop = (start, pred) -> {
            buf.limit(start);

            do {
                buf.position(start);
                buf.limit(buf.limit() + 1);
            }
            while (!pred.apply(buf));
        };

        bufLoop.apply(0, buf0 -> srcMsg.writeTo(buf0, new DirectMessageWriter(PROTO_VER)));

        buf.flip();

        byte b0 = buf.get();
        byte b1 = buf.get();

        short type = (short)((b1 & 0xFF) << 8 | b0 & 0xFF);

        assertEquals(srcMsg.directType(), type);

        SingleNodeMessage<?> resMsg = (SingleNodeMessage<?>)msgFactory.create(type);

        bufLoop.apply(buf.position(), buf0 -> resMsg.readFrom(buf0, new DirectMessageReader(msgFactory, PROTO_VER)));

        assertEquals(srcMsg.type(), resMsg.type());
        assertEquals(srcMsg.processId(), resMsg.processId());
        assertEquals(srcMsg.response(), resMsg.response());

        Exception expErr = srcMsg.error();
        Exception resErr = resMsg.error();

        if (expErr == null)
            assertNull(resErr);
        else {
            assertEquals(expErr.getClass(), resErr.getClass());
            assertEquals(expErr.getMessage(), resErr.getMessage());
        }
    }
}
