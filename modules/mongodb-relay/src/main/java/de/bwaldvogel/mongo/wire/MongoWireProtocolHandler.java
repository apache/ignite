package de.bwaldvogel.mongo.wire;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.backend.Assert;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.bson.BsonDecoder;
import de.bwaldvogel.mongo.wire.message.ClientRequest;
import de.bwaldvogel.mongo.wire.message.MessageHeader;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * Based on information from
 * <a href="https://docs.mongodb.org/manual/reference/mongodb-wire-protocol/">https://docs.mongodb.org/manual/reference/mongodb-wire-protocol/</a>
 */
public class MongoWireProtocolHandler extends LengthFieldBasedFrameDecoder {

    public static final int MAX_MESSAGE_SIZE_BYTES = 48 * 1000 * 1000;

    public static final int MAX_WRITE_BATCH_SIZE = 1000;

    private static final Logger log = LoggerFactory.getLogger(MongoWireProtocolHandler.class);

    private static final int maxFrameLength = Integer.MAX_VALUE;
    private static final int lengthFieldOffset = 0;
    private static final int lengthFieldLength = 4;
    private static final int lengthAdjustment = -lengthFieldLength;
    private static final int initialBytesToStrip = 0;

    public MongoWireProtocolHandler() {
        super(maxFrameLength, lengthFieldOffset, lengthFieldLength, lengthAdjustment, initialBytesToStrip);
    }

    @Override
    protected ClientRequest decode(ChannelHandlerContext ctx, ByteBuf buf) throws Exception {

        ByteBuf in = buf;

        if (in.readableBytes() < 4) {
            return null;
        }

        in.markReaderIndex();
        int totalLength = in.readIntLE();

        if (totalLength > MAX_MESSAGE_SIZE_BYTES) {
            throw new IOException("message too large: " + totalLength + " bytes");
        }

        if (in.readableBytes() < totalLength - lengthFieldLength) {
            in.resetReaderIndex();
            return null; // retry
        }
        in = in.readSlice(totalLength - lengthFieldLength);
        int readable = in.readableBytes();
        Assert.equals(readable, (long) totalLength - lengthFieldLength);

        final int requestID = in.readIntLE();
        final int responseTo = in.readIntLE();
        final MessageHeader header = new MessageHeader(requestID, responseTo);

        int opCodeId = in.readIntLE();
        final OpCode opCode = OpCode.getById(opCodeId);
        if (opCode == null) {
            throw new IOException("opCode " + opCodeId + " not supported");
        }

        final Channel channel = ctx.channel();
        final ClientRequest request;

        switch (opCode) {
            case OP_QUERY:
                request = handleQuery(channel, header, in);
                break;
            case OP_INSERT:
                request = handleInsert(channel, header, in);
                break;
            case OP_DELETE:
                request = handleDelete(channel, header, in);
                break;
            case OP_UPDATE:
                request = handleUpdate(channel, header, in);
                break;
            default:
                throw new UnsupportedOperationException("unsupported opcode: " + opCode);
        }

        if (in.isReadable()) {
            throw new IOException();
        }

        log.debug("{}", request);

        return request;
    }

    private ClientRequest handleDelete(Channel channel, MessageHeader header, ByteBuf buffer) throws IOException {

        buffer.skipBytes(4); // reserved

        final String fullCollectionName = BsonDecoder.decodeCString(buffer);

        final int flags = buffer.readIntLE();
        boolean singleRemove = false;
        if (flags == 0) {
            // ignore
        } else if (flags == 1) {
            singleRemove = true;
        } else {
            throw new UnsupportedOperationException("flags=" + flags + " not yet supported");
        }

        Document selector = BsonDecoder.decodeBson(buffer);
        log.debug("delete {} from {}", selector, fullCollectionName);
        return new MongoDelete(channel, header, fullCollectionName, selector, singleRemove);
    }

    private ClientRequest handleUpdate(Channel channel, MessageHeader header, ByteBuf buffer) throws IOException {

        buffer.skipBytes(4); // reserved

        final String fullCollectionName = BsonDecoder.decodeCString(buffer);

        final int flags = buffer.readIntLE();
        boolean upsert = UpdateFlag.UPSERT.isSet(flags);
        boolean multi = UpdateFlag.MULTI_UPDATE.isSet(flags);

        Document selector = BsonDecoder.decodeBson(buffer);
        Document update = BsonDecoder.decodeBson(buffer);
        log.debug("update {} in {}", selector, fullCollectionName);
        return new MongoUpdate(channel, header, fullCollectionName, selector, update, upsert, multi);
    }

    private ClientRequest handleInsert(Channel channel, MessageHeader header, ByteBuf buffer) throws IOException {

        final int flags = buffer.readIntLE();
        if (flags != 0) {
            throw new UnsupportedOperationException("flags=" + flags + " not yet supported");
        }

        final String fullCollectionName = BsonDecoder.decodeCString(buffer);

        List<Document> documents = new ArrayList<>();
        while (buffer.isReadable()) {
            Document document = BsonDecoder.decodeBson(buffer);
            documents.add(document);
        }
        log.debug("insert {} in {}", documents, fullCollectionName);
        return new MongoInsert(channel, header, fullCollectionName, documents);
    }

    private ClientRequest handleQuery(Channel channel, MessageHeader header, ByteBuf buffer) throws IOException {

        int flags = buffer.readIntLE();

        final String fullCollectionName = BsonDecoder.decodeCString(buffer);
        final int numberToSkip = buffer.readIntLE();
        final int numberToReturn = buffer.readIntLE();

        Document query = BsonDecoder.decodeBson(buffer);
        Document returnFieldSelector = null;
        if (buffer.isReadable()) {
            returnFieldSelector = BsonDecoder.decodeBson(buffer);
        }

        MongoQuery mongoQuery = new MongoQuery(channel, header, fullCollectionName, numberToSkip, numberToReturn,
            query, returnFieldSelector);

        if (QueryFlag.SLAVE_OK.isSet(flags)) {
            flags = QueryFlag.SLAVE_OK.removeFrom(flags);
        }

        if (QueryFlag.NO_CURSOR_TIMEOUT.isSet(flags)) {
            flags = QueryFlag.NO_CURSOR_TIMEOUT.removeFrom(flags);
        }

        if (flags != 0) {
            throw new UnsupportedOperationException("flags=" + flags + " not yet supported");
        }

        log.debug("query {} from {}", query, fullCollectionName);

        return mongoQuery;
    }

}
