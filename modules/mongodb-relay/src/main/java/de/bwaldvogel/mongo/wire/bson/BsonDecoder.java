package de.bwaldvogel.mongo.wire.bson;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import de.bwaldvogel.mongo.bson.BsonJavaScript;
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.Decimal128;
import de.bwaldvogel.mongo.bson.Document;

import de.bwaldvogel.mongo.bson.MaxKey;
import de.bwaldvogel.mongo.bson.MinKey;
import de.bwaldvogel.mongo.bson.ObjectId;
import de.bwaldvogel.mongo.wire.BsonConstants;
import io.netty.buffer.ByteBuf;

public final class BsonDecoder {

    private BsonDecoder() {
    }

    public static Document decodeBson(ByteBuf buffer) {
        final int totalObjectLength = buffer.readIntLE();
        final int length = totalObjectLength - 4;
        if (buffer.readableBytes() < length) {
            throw new IllegalArgumentException("Too few bytes to read: " + buffer.readableBytes() + ". Expected: " + length);
        }
        if (length > BsonConstants.MAX_BSON_OBJECT_SIZE) {
            throw new IllegalArgumentException("BSON object too large: " + length + " bytes");
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
        throw new IllegalArgumentException("illegal BSON object. Terminating byte not found. totalObjectLength = " + totalObjectLength);
    }

    public static Object decodeValue(byte type, ByteBuf buffer) {
        switch (type) {
            case BsonConstants.TYPE_DOUBLE:
                return Double.valueOf(Double.longBitsToDouble(buffer.readLongLE()));
            case BsonConstants.TYPE_UTF8_STRING:
                return decodeString(buffer);
            case BsonConstants.TYPE_EMBEDDED_DOCUMENT:
                return decodeBson(buffer);
            case BsonConstants.TYPE_ARRAY:
                return decodeArray(buffer);
            case BsonConstants.TYPE_DATA:
                return decodeBinary(buffer);
            case BsonConstants.TYPE_UNDEFINED:
            case BsonConstants.TYPE_NULL:
                return null;
            case BsonConstants.TYPE_OBJECT_ID:
                return decodeObjectId(buffer);
            case BsonConstants.TYPE_BOOLEAN:
                return decodeBoolean(buffer);
            case BsonConstants.TYPE_UTC_DATETIME:
            	//-return new Date(buffer.readLongLE());
                return Instant.ofEpochMilli(buffer.readLongLE());
            case BsonConstants.TYPE_REGEX:
                return decodePattern(buffer);
            case BsonConstants.TYPE_INT32:
                return Integer.valueOf(buffer.readIntLE());
            case BsonConstants.TYPE_TIMESTAMP:    
            	return new Timestamp(buffer.readLongLE());
                //-return new BsonTimestamp(buffer.readLongLE());
            case BsonConstants.TYPE_INT64:
                return Long.valueOf(buffer.readLongLE());
            case BsonConstants.TYPE_DECIMAL128:
                return new Decimal128(buffer.readLongLE(), buffer.readLongLE());
            case BsonConstants.TYPE_MAX_KEY:
                return MaxKey.getInstance();
            case BsonConstants.TYPE_MIN_KEY:
                return MinKey.getInstance();
            case BsonConstants.TYPE_JAVASCRIPT_CODE:
                return new BsonJavaScript(decodeString(buffer));
            case BsonConstants.TYPE_JAVASCRIPT_CODE_WITH_SCOPE:
                throw new IllegalArgumentException("unhandled type: 0x" + Integer.toHexString(type));
            default:
                throw new IllegalArgumentException("unknown type: 0x" + Integer.toHexString(type));
        }
    }

    private static BsonRegularExpression decodePattern(ByteBuf buffer) {
        String regex = decodeCString(buffer);
        String options = decodeCString(buffer);
        return new BsonRegularExpression(regex, options);
    }

    private static List<Object> decodeArray(ByteBuf buffer) {
        List<Object> array = new ArrayList<>();
        Document arrayObject = decodeBson(buffer);
        for (String key : arrayObject.keySet()) {
            array.add(arrayObject.get(key));
        }
        return array;
    }

    private static ObjectId decodeObjectId(ByteBuf buffer) {
        byte[] b = new byte[BsonConstants.LENGTH_OBJECTID];
        buffer.readBytes(b);
        return new ObjectId(b);
    }

    private static String decodeString(ByteBuf buffer) {
        int length = buffer.readIntLE();
        byte[] data = new byte[length - 1];
        buffer.readBytes(data);
        String value = new String(data, StandardCharsets.UTF_8);
        byte trail = buffer.readByte();
        if (trail != BsonConstants.STRING_TERMINATION) {
            throw new IllegalArgumentException("Unexpected trailing byte: " + trail);
        }
        return value;
    }

    // default visibility for unit test
    public static String decodeCString(ByteBuf buffer) {
        int length = buffer.bytesBefore(BsonConstants.STRING_TERMINATION);
        if (length < 0)
            throw new IllegalArgumentException("string termination not found");

        String result = buffer.toString(buffer.readerIndex(), length, StandardCharsets.UTF_8);
        buffer.skipBytes(length + 1);
        return result;
    }

    private static Object decodeBinary(ByteBuf buffer) {
        int length = buffer.readIntLE();
        int subtype = buffer.readByte();
        switch (subtype) {
            case BsonConstants.BINARY_SUBTYPE_GENERIC:
            case BsonConstants.BINARY_SUBTYPE_USER_DEFINED: {
                byte[] data = new byte[length];
                buffer.readBytes(data);
                return data;
            }
            case BsonConstants.BINARY_SUBTYPE_OLD_UUID: {
                if (length != BsonConstants.LENGTH_UUID) {
                    throw new IllegalArgumentException("Illegal length: " + length);
                }
                return new UUID(buffer.readLongLE(), buffer.readLongLE());
            }
            case BsonConstants.BINARY_SUBTYPE_UUID: {
                if (length != BsonConstants.LENGTH_UUID) {
                    throw new IllegalArgumentException("Illegal length: " + length);
                }
                return new UUID(buffer.readLong(), buffer.readLong());
            }
            default:
                throw new IllegalArgumentException("Unknown subtype: " + subtype);
        }
    }

    private static Object decodeBoolean(ByteBuf buffer) {
        byte value = buffer.readByte();
        switch (value) {
            case BsonConstants.BOOLEAN_VALUE_FALSE:
                return Boolean.FALSE;
            case BsonConstants.BOOLEAN_VALUE_TRUE:
                return Boolean.TRUE;
            default:
                throw new IllegalArgumentException("Illegal boolean value:" + value);
        }
    }

}
