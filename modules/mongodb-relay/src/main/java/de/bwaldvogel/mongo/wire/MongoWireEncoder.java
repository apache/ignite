package de.bwaldvogel.mongo.wire;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.bson.BsonEncoder;
import de.bwaldvogel.mongo.wire.message.MongoReply;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class MongoWireEncoder extends MessageToByteEncoder<MongoReply> {

    private static final Logger log = LoggerFactory.getLogger(MongoWireEncoder.class);

    @Override
    protected void encode(ChannelHandlerContext ctx, MongoReply reply, ByteBuf buf) throws Exception {

        buf.writeIntLE(0); // write length later

        buf.writeIntLE(reply.getHeader().getRequestID());
        buf.writeIntLE(reply.getHeader().getResponseTo());
        buf.writeIntLE(OpCode.OP_REPLY.getId());

        buf.writeIntLE(reply.getFlags());
        buf.writeLongLE(reply.getCursorId());
        buf.writeIntLE(reply.getStartingFrom());
        final List<Document> documents = reply.getDocuments();
        buf.writeIntLE(documents.size());

        for (Document document : documents) {
            try {
                BsonEncoder.encodeDocument(document, buf);
            } catch (RuntimeException e) {
                log.error("Failed to encode {}", document, e);
                ctx.channel().close();
                throw e;
            }
        }

        log.debug("wrote reply: {}", reply);

        // now set the length
        final int writerIndex = buf.writerIndex();
        buf.setIntLE(0, writerIndex);
    }
}
