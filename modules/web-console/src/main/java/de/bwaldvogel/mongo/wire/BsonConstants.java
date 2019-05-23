package de.bwaldvogel.mongo.wire;

public interface BsonConstants {
    int MAX_BSON_OBJECT_SIZE = 16 * 1024 * 1024;

    byte TERMINATING_BYTE = 0x00;
    byte TYPE_DOUBLE = 0x01;
    byte TYPE_UTF8_STRING = 0x02;
    byte TYPE_EMBEDDED_DOCUMENT = 0x03;
    byte TYPE_ARRAY = 0x04;
    byte TYPE_DATA = 0x05;
    /**
     * Deprecated
     */
    byte TYPE_UNDEFINED = 0x06;
    byte TYPE_OBJECT_ID = 0x07;
    byte TYPE_BOOLEAN = 0x08;
    byte TYPE_UTC_DATETIME = 0x09;
    byte TYPE_NULL = 0x0A;
    byte TYPE_REGEX = 0x0B;
    /**
     * Deprecated
     */
    byte TYPE_DBPOINTER = 0x0C;
    byte TYPE_JAVASCRIPT_CODE = 0x0D;
    /**
     * Deprecated
     */
    byte TYPE_SYMBOL = 0x0E;
    byte TYPE_JAVASCRIPT_CODE_WITH_SCOPE = 0x0F;
    byte TYPE_INT32 = 0x10;
    byte TYPE_TIMESTAMP = 0x11;
    byte TYPE_INT64 = 0x12;
    byte TYPE_DECIMAL128 = 0x13;
    byte TYPE_MIN_KEY = (byte) 0xFF;
    byte TYPE_MAX_KEY = 0x7F;

    byte BOOLEAN_VALUE_FALSE = 0x00;
    byte BOOLEAN_VALUE_TRUE = 0x01;

    byte STRING_TERMINATION = 0x00;

    byte BINARY_SUBTYPE_GENERIC = 0x00;
    byte BINARY_SUBTYPE_FUNCTION = 0x01;
    byte BINARY_SUBTYPE_OLD_BINARY = 0x02;
    byte BINARY_SUBTYPE_OLD_UUID = 0x03;
    byte BINARY_SUBTYPE_UUID = 0x04;
    byte BINARY_SUBTYPE_MD5 = 0x05;
    byte BINARY_SUBTYPE_USER_DEFINED = (byte) 0x80;

    /**
     * Length in bytes
     */
    int LENGTH_UUID = 128 / 8;

    /**
     * Length in bytes
     */
    int LENGTH_OBJECTID = 12;

}
