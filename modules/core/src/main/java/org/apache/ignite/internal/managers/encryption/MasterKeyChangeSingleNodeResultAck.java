package org.apache.ignite.internal.managers.encryption;

import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 * This message is sent by the server node upon local completion of the master key change process. The initiator node
 * collects all node's acks to guarantee the process completed at all nodes.
 *
 * @see GridEncryptionManager.MasterKeyChangeProcess
 */
public class MasterKeyChangeSingleNodeResultAck implements SingleNodeMessage {
    /** Request id. */
    private UUID reqId;

    /** Error. */
    private String err;

    /**
     * Empty constructor for marshalling purposes.
     */
    public MasterKeyChangeSingleNodeResultAck() {
    }

    /**
     * @param reqId Request id.
     */
    public MasterKeyChangeSingleNodeResultAck(UUID reqId, String err) {
        this.reqId = reqId;
        this.err = err;
    }

    /** @return Error. */
    public String error() {
        return err;
    }

    /** @return {@code True} if has error. */
    public boolean hasError() {
        return err != null;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeUuid("reqId", reqId))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeString("err", err))
                    return false;

                writer.incrementState();
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                reqId = reader.readUuid("reqId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                err = reader.readString("err");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();
        }

        return reader.afterMessageRead(MasterKeyChangeSingleNodeResultAck.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 178;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public UUID requestId() {
        return reqId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MasterKeyChangeSingleNodeResultAck.class, this, "reqId", reqId);
    }
}
