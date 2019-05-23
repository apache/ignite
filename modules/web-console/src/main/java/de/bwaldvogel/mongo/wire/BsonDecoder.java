package de.bwaldvogel.mongo.wire;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import de.bwaldvogel.mongo.bson.BsonJavaScript;
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Decimal128;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.MaxKey;
import de.bwaldvogel.mongo.bson.MinKey;
import de.bwaldvogel.mongo.bson.ObjectId;
import io.netty.buffer.ByteBuf;

class BsonDecoder {

    Document decodeBson(ByteBuf buffer) throws IOException {
        final int totalObjectLength = buffer.readIntLE();
        final int length = totalObjectLength - 4;
        if (buffer.readableBytes() < length) {
            throw new IOException("Too few bytes to read: " + buffer.readableBytes() + ". Expected: " + length);
        }
        if (length > BsonConstants.MAX_BSON_OBJECT_SIZE) {
            throw new IOException("BSON object too large: " + length + " bytes");
        }

        Document object = new Document();
        int start = buffer.readerIndex();
        while (buffer.readerIndex() - start < length) {
            byte type = buffer.readByte();
            if (type == BsonConstants.TERMINATING_BYTE) {
                return object;
            }
            String name = decodeCString(buffer);
            Object value = decodeValue(type, buffer);
            object.put(name, value);
        }
        throw new IOException("illegal BSON object. Terminating byte not found. totalObjectLength = " + totalObjectLength);
    }

    private Object decodeValue(byte type, ByteBuf buffer) throws IOException {
        Object value;
        switch (type) {
            case BsonConstants.TYPE_DOUBLE:
                value = Double.valueOf(Double.longBitsToDouble(buffer.readLongLE()));
                break;
            case BsonConstants.TYPE_UTF8_STRING:
                value = decodeString(buffer);
                break;
            case BsonConstants.TYPE_EMBEDDED_DOCUMENT:
                value = decodeBson(buffer);
                break;
            case BsonConstants.TYPE_ARRAY:
                value = decodeArray(buffer);
                break;
            case BsonConstants.TYPE_DATA:
                value = decodeBinary(buffer);
                break;
            case BsonConstants.TYPE_UNDEFINED:
                value = null;
                break;
            case BsonConstants.TYPE_OBJECT_ID:
                value = decodeObjectId(buffer);
                break;
            case BsonConstants.TYPE_BOOLEAN:
                value = decodeBoolean(buffer);
                break;
            case BsonConstants.TYPE_UTC_DATETIME:
                value = new Date(buffer.readLongLE());
                break;
            case BsonConstants.TYPE_NULL:
                value = null;
                break;
            case BsonConstants.TYPE_REGEX:
                value = decodePattern(buffer);
                break;
            case BsonConstants.TYPE_INT32:
                value = Integer.valueOf(buffer.readIntLE());
                break;
            case BsonConstants.TYPE_TIMESTAMP:
                value = new BsonTimestamp(buffer.readLongLE());
                break;
            case BsonConstants.TYPE_INT64:
                value = Long.valueOf(buffer.readLongLE());
                break;
            case BsonConstants.TYPE_DECIMAL128:
                value = new Decimal128(buffer.readLongLE(), buffer.readLongLE());
                break;
            case BsonConstants.TYPE_MAX_KEY:
                value = MaxKey.getInstance();
                break;
            case BsonConstants.TYPE_MIN_KEY:
                value = MinKey.getInstance();
                break;
            case BsonConstants.TYPE_JAVASCRIPT_CODE:
                value = new BsonJavaScript(decodeString(buffer));
                break;
            case BsonConstants.TYPE_JAVASCRIPT_CODE_WITH_SCOPE:
                throw new IOException("unhandled type: 0x" + Integer.toHexString(type));
            default:
                throw new IOException("unknown type: 0x" + Integer.toHexString(type));
        }
        return value;
    }

    private BsonRegularExpression decodePattern(ByteBuf buffer) throws IOException {
        String regex = decodeCString(buffer);
        String options = decodeCString(buffer);
        return new BsonRegularExpression(regex, options);
    }

    private List<Object> decodeArray(ByteBuf buffer) throws IOException {
        List<Object> array = new ArrayList<>();
        Document arrayObject = decodeBson(buffer);
        for (String key : arrayObject.keySet()) {
            array.add(arrayObject.get(key));
        }
        return array;
    }

    private ObjectId decodeObjectId(ByteBuf buffer) {
        byte[] b = new byte[BsonConstants.LENGTH_OBJECTID];
        buffer.readBytes(b);
        return new ObjectId(b);
    }

    private String decodeString(ByteBuf buffer) throws IOException {
        int length = buffer.readIntLE();
        byte[] data = new byte[length - 1];
        buffer.readBytes(data);
        String value = new String(data, StandardCharsets.UTF_8);
        byte trail = buffer.readByte();
        if (trail != BsonConstants.STRING_TERMINATION) {
            throw new IOException();
        }
        return value;
    }

    // default visibility for unit test
    String decodeCString(ByteBuf buffer) throws IOException {
        int length = buffer.bytesBefore(BsonConstants.STRING_TERMINATION);
        if (length < 0)
            throw new IOException("string termination not found");

        String result = buffer.toString(buffer.readerIndex(), length, StandardCharsets.UTF_8);
        buffer.skipBytes(length + 1);
        return result;
    }

    private Object decodeBinary(ByteBuf buffer) throws IOException {
        int length = buffer.readIntLE();
        int subtype = buffer.readByte();
        switch (subtype) {
            case BsonConstants.BINARY_SUBTYPE_GENERIC:
            case BsonConstants.BINARY_SUBTYPE_USER_DEFINED: {
                byte[] data = new byte[length];
                buffer.readBytes(data);
                return data;
            }
            case BsonConstants.BINARY_SUBTYPE_OLD_UUID:
            case BsonConstants.BINARY_SUBTYPE_UUID: {
                if (length != BsonConstants.LENGTH_UUID) {
                    throw new IOException();
                }
                return new UUID(buffer.readLongLE(), buffer.readLongLE());
            }
            default:
                throw new IOException();
        }
    }

    private Object decodeBoolean(ByteBuf buffer) throws IOException {
        switch (buffer.readByte()) {
            case BsonConstants.BOOLEAN_VALUE_FALSE:
                return Boolean.FALSE;
            case BsonConstants.BOOLEAN_VALUE_TRUE:
                return Boolean.TRUE;
            default:
                throw new IOException("illegal boolean value");
        }
    }

}
