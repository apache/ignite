/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.h2.engine.Constants;

/**
 * Utility methods
 */
public final class DataUtils {

    /**
     * An error occurred while reading from the file.
     */
    public static final int ERROR_READING_FAILED = 1;

    /**
     * An error occurred when trying to write to the file.
     */
    public static final int ERROR_WRITING_FAILED = 2;

    /**
     * An internal error occurred. This could be a bug, or a memory corruption
     * (for example caused by out of memory).
     */
    public static final int ERROR_INTERNAL = 3;

    /**
     * The object is already closed.
     */
    public static final int ERROR_CLOSED = 4;

    /**
     * The file format is not supported.
     */
    public static final int ERROR_UNSUPPORTED_FORMAT = 5;

    /**
     * The file is corrupt or (for encrypted files) the encryption key is wrong.
     */
    public static final int ERROR_FILE_CORRUPT = 6;

    /**
     * The file is locked.
     */
    public static final int ERROR_FILE_LOCKED = 7;

    /**
     * An error occurred when serializing or de-serializing.
     */
    public static final int ERROR_SERIALIZATION = 8;

    /**
     * The application was trying to read data from a chunk that is no longer
     * available.
     */
    public static final int ERROR_CHUNK_NOT_FOUND = 9;

    /**
     * The block in the stream store was not found.
     */
    public static final int ERROR_BLOCK_NOT_FOUND = 50;

    /**
     * The transaction store is corrupt.
     */
    public static final int ERROR_TRANSACTION_CORRUPT = 100;

    /**
     * An entry is still locked by another transaction.
     */
    public static final int ERROR_TRANSACTION_LOCKED = 101;

    /**
     * There are too many open transactions.
     */
    public static final int ERROR_TOO_MANY_OPEN_TRANSACTIONS = 102;

    /**
     * The transaction store is in an illegal state (for example, not yet
     * initialized).
     */
    public static final int ERROR_TRANSACTION_ILLEGAL_STATE = 103;

    /**
     * The type for leaf page.
     */
    public static final int PAGE_TYPE_LEAF = 0;

    /**
     * The type for node page.
     */
    public static final int PAGE_TYPE_NODE = 1;

    /**
     * The bit mask for compressed pages (compression level fast).
     */
    public static final int PAGE_COMPRESSED = 2;

    /**
     * The bit mask for compressed pages (compression level high).
     */
    public static final int PAGE_COMPRESSED_HIGH = 2 + 4;

    /**
     * The maximum length of a variable size int.
     */
    public static final int MAX_VAR_INT_LEN = 5;

    /**
     * The maximum length of a variable size long.
     */
    public static final int MAX_VAR_LONG_LEN = 10;

    /**
     * The maximum integer that needs less space when using variable size
     * encoding (only 3 bytes instead of 4).
     */
    public static final int COMPRESSED_VAR_INT_MAX = 0x1fffff;

    /**
     * The maximum long that needs less space when using variable size
     * encoding (only 7 bytes instead of 8).
     */
    public static final long COMPRESSED_VAR_LONG_MAX = 0x1ffffffffffffL;

    /**
     * The estimated number of bytes used per page object.
     */
    public static final int PAGE_MEMORY = 128;

    /**
     * The estimated number of bytes used per child entry.
     */
    public static final int PAGE_MEMORY_CHILD = 16;

    /**
     * The marker size of a very large page.
     */
    public static final int PAGE_LARGE = 2 * 1024 * 1024;

    /**
     * Get the length of the variable size int.
     *
     * @param x the value
     * @return the length in bytes
     */
    public static int getVarIntLen(int x) {
        if ((x & (-1 << 7)) == 0) {
            return 1;
        } else if ((x & (-1 << 14)) == 0) {
            return 2;
        } else if ((x & (-1 << 21)) == 0) {
            return 3;
        } else if ((x & (-1 << 28)) == 0) {
            return 4;
        }
        return 5;
    }

    /**
     * Get the length of the variable size long.
     *
     * @param x the value
     * @return the length in bytes
     */
    public static int getVarLongLen(long x) {
        int i = 1;
        while (true) {
            x >>>= 7;
            if (x == 0) {
                return i;
            }
            i++;
        }
    }

    /**
     * Read a variable size int.
     *
     * @param buff the source buffer
     * @return the value
     */
    public static int readVarInt(ByteBuffer buff) {
        int b = buff.get();
        if (b >= 0) {
            return b;
        }
        // a separate function so that this one can be inlined
        return readVarIntRest(buff, b);
    }

    private static int readVarIntRest(ByteBuffer buff, int b) {
        int x = b & 0x7f;
        b = buff.get();
        if (b >= 0) {
            return x | (b << 7);
        }
        x |= (b & 0x7f) << 7;
        b = buff.get();
        if (b >= 0) {
            return x | (b << 14);
        }
        x |= (b & 0x7f) << 14;
        b = buff.get();
        if (b >= 0) {
            return x | b << 21;
        }
        x |= ((b & 0x7f) << 21) | (buff.get() << 28);
        return x;
    }

    /**
     * Read a variable size long.
     *
     * @param buff the source buffer
     * @return the value
     */
    public static long readVarLong(ByteBuffer buff) {
        long x = buff.get();
        if (x >= 0) {
            return x;
        }
        x &= 0x7f;
        for (int s = 7; s < 64; s += 7) {
            long b = buff.get();
            x |= (b & 0x7f) << s;
            if (b >= 0) {
                break;
            }
        }
        return x;
    }

    /**
     * Write a variable size int.
     *
     * @param out the output stream
     * @param x the value
     */
    public static void writeVarInt(OutputStream out, int x) throws IOException {
        while ((x & ~0x7f) != 0) {
            out.write((byte) (0x80 | (x & 0x7f)));
            x >>>= 7;
        }
        out.write((byte) x);
    }

    /**
     * Write a variable size int.
     *
     * @param buff the source buffer
     * @param x the value
     */
    public static void writeVarInt(ByteBuffer buff, int x) {
        while ((x & ~0x7f) != 0) {
            buff.put((byte) (0x80 | (x & 0x7f)));
            x >>>= 7;
        }
        buff.put((byte) x);
    }

    /**
     * Write characters from a string (without the length).
     *
     * @param buff the target buffer (must be large enough)
     * @param s the string
     * @param len the number of characters
     */
    public static void writeStringData(ByteBuffer buff,
            String s, int len) {
        for (int i = 0; i < len; i++) {
            int c = s.charAt(i);
            if (c < 0x80) {
                buff.put((byte) c);
            } else if (c >= 0x800) {
                buff.put((byte) (0xe0 | (c >> 12)));
                buff.put((byte) (((c >> 6) & 0x3f)));
                buff.put((byte) (c & 0x3f));
            } else {
                buff.put((byte) (0xc0 | (c >> 6)));
                buff.put((byte) (c & 0x3f));
            }
        }
    }

    /**
     * Read a string.
     *
     * @param buff the source buffer
     * @param len the number of characters
     * @return the value
     */
    public static String readString(ByteBuffer buff, int len) {
        char[] chars = new char[len];
        for (int i = 0; i < len; i++) {
            int x = buff.get() & 0xff;
            if (x < 0x80) {
                chars[i] = (char) x;
            } else if (x >= 0xe0) {
                chars[i] = (char) (((x & 0xf) << 12)
                        + ((buff.get() & 0x3f) << 6) + (buff.get() & 0x3f));
            } else {
                chars[i] = (char) (((x & 0x1f) << 6) + (buff.get() & 0x3f));
            }
        }
        return new String(chars);
    }

    /**
     * Write a variable size long.
     *
     * @param buff the target buffer
     * @param x the value
     */
    public static void writeVarLong(ByteBuffer buff, long x) {
        while ((x & ~0x7f) != 0) {
            buff.put((byte) (0x80 | (x & 0x7f)));
            x >>>= 7;
        }
        buff.put((byte) x);
    }

    /**
     * Write a variable size long.
     *
     * @param out the output stream
     * @param x the value
     */
    public static void writeVarLong(OutputStream out, long x)
            throws IOException {
        while ((x & ~0x7f) != 0) {
            out.write((byte) (0x80 | (x & 0x7f)));
            x >>>= 7;
        }
        out.write((byte) x);
    }

    /**
     * Copy the elements of an array, with a gap.
     *
     * @param src the source array
     * @param dst the target array
     * @param oldSize the size of the old array
     * @param gapIndex the index of the gap
     */
    public static void copyWithGap(Object src, Object dst, int oldSize,
            int gapIndex) {
        if (gapIndex > 0) {
            System.arraycopy(src, 0, dst, 0, gapIndex);
        }
        if (gapIndex < oldSize) {
            System.arraycopy(src, gapIndex, dst, gapIndex + 1, oldSize
                    - gapIndex);
        }
    }

    /**
     * Copy the elements of an array, and remove one element.
     *
     * @param src the source array
     * @param dst the target array
     * @param oldSize the size of the old array
     * @param removeIndex the index of the entry to remove
     */
    public static void copyExcept(Object src, Object dst, int oldSize,
            int removeIndex) {
        if (removeIndex > 0 && oldSize > 0) {
            System.arraycopy(src, 0, dst, 0, removeIndex);
        }
        if (removeIndex < oldSize) {
            System.arraycopy(src, removeIndex + 1, dst, removeIndex, oldSize
                    - removeIndex - 1);
        }
    }

    /**
     * Read from a file channel until the buffer is full.
     * The buffer is rewind after reading.
     *
     * @param file the file channel
     * @param pos the absolute position within the file
     * @param dst the byte buffer
     * @throws IllegalStateException if some data could not be read
     */
    public static void readFully(FileChannel file, long pos, ByteBuffer dst) {
        try {
            do {
                int len = file.read(dst, pos);
                if (len < 0) {
                    throw new EOFException();
                }
                pos += len;
            } while (dst.remaining() > 0);
            dst.rewind();
        } catch (IOException e) {
            long size;
            try {
                size = file.size();
            } catch (IOException e2) {
                size = -1;
            }
            throw newIllegalStateException(
                    ERROR_READING_FAILED,
                    "Reading from {0} failed; file length {1} " +
                    "read length {2} at {3}",
                    file, size, dst.remaining(), pos, e);
        }
    }

    /**
     * Write to a file channel.
     *
     * @param file the file channel
     * @param pos the absolute position within the file
     * @param src the source buffer
     */
    public static void writeFully(FileChannel file, long pos, ByteBuffer src) {
        try {
            int off = 0;
            do {
                int len = file.write(src, pos + off);
                off += len;
            } while (src.remaining() > 0);
        } catch (IOException e) {
            throw newIllegalStateException(
                    ERROR_WRITING_FAILED,
                    "Writing to {0} failed; length {1} at {2}",
                    file, src.remaining(), pos, e);
        }
    }

    /**
     * Convert the length to a length code 0..31. 31 means more than 1 MB.
     *
     * @param len the length
     * @return the length code
     */
    public static int encodeLength(int len) {
        if (len <= 32) {
            return 0;
        }
        int code = Integer.numberOfLeadingZeros(len);
        int remaining = len << (code + 1);
        code += code;
        if ((remaining & (1 << 31)) != 0) {
            code--;
        }
        if ((remaining << 1) != 0) {
            code--;
        }
        code = Math.min(31, 52 - code);
        // alternative code (slower):
        // int x = len;
        // int shift = 0;
        // while (x > 3) {
        //    shift++;
        //    x = (x >>> 1) + (x & 1);
        // }
        // shift = Math.max(0,  shift - 4);
        // int code = (shift << 1) + (x & 1);
        // code = Math.min(31, code);
        return code;
    }

    /**
     * Get the chunk id from the position.
     *
     * @param pos the position
     * @return the chunk id
     */
    public static int getPageChunkId(long pos) {
        return (int) (pos >>> 38);
    }

    /**
     * Get the maximum length for the given code.
     * For the code 31, PAGE_LARGE is returned.
     *
     * @param pos the position
     * @return the maximum length
     */
    public static int getPageMaxLength(long pos) {
        int code = (int) ((pos >> 1) & 31);
        if (code == 31) {
            return PAGE_LARGE;
        }
        return (2 + (code & 1)) << ((code >> 1) + 4);
    }

    /**
     * Get the offset from the position.
     *
     * @param pos the position
     * @return the offset
     */
    public static int getPageOffset(long pos) {
        return (int) (pos >> 6);
    }

    /**
     * Get the page type from the position.
     *
     * @param pos the position
     * @return the page type (PAGE_TYPE_NODE or PAGE_TYPE_LEAF)
     */
    public static int getPageType(long pos) {
        return ((int) pos) & 1;
    }

    /**
     * Get the position of this page. The following information is encoded in
     * the position: the chunk id, the offset, the maximum length, and the type
     * (node or leaf).
     *
     * @param chunkId the chunk id
     * @param offset the offset
     * @param length the length
     * @param type the page type (1 for node, 0 for leaf)
     * @return the position
     */
    public static long getPagePos(int chunkId, int offset,
            int length, int type) {
        long pos = (long) chunkId << 38;
        pos |= (long) offset << 6;
        pos |= encodeLength(length) << 1;
        pos |= type;
        return pos;
    }

    /**
     * Calculate a check value for the given integer. A check value is mean to
     * verify the data is consistent with a high probability, but not meant to
     * protect against media failure or deliberate changes.
     *
     * @param x the value
     * @return the check value
     */
    public static short getCheckValue(int x) {
        return (short) ((x >> 16) ^ x);
    }

    /**
     * Append a map to the string builder, sorted by key.
     *
     * @param buff the target buffer
     * @param map the map
     * @return the string builder
     */
    public static StringBuilder appendMap(StringBuilder buff, HashMap<String, ?> map) {
        Object[] keys = map.keySet().toArray();
        Arrays.sort(keys);
        for (Object k : keys) {
            String key = (String) k;
            Object value = map.get(key);
            if (value instanceof Long) {
                appendMap(buff, key, (long) value);
            } else if (value instanceof Integer) {
                appendMap(buff, key, (int) value);
            } else {
                appendMap(buff, key, value.toString());
            }
        }
        return buff;
    }

    private static StringBuilder appendMapKey(StringBuilder buff, String key) {
        if (buff.length() > 0) {
            buff.append(',');
        }
        return buff.append(key).append(':');
    }

    /**
     * Append a key-value pair to the string builder. Keys may not contain a
     * colon. Values that contain a comma or a double quote are enclosed in
     * double quotes, with special characters escaped using a backslash.
     *
     * @param buff the target buffer
     * @param key the key
     * @param value the value
     */
    public static void appendMap(StringBuilder buff, String key, String value) {
        appendMapKey(buff, key);
        if (value.indexOf(',') < 0 && value.indexOf('\"') < 0) {
            buff.append(value);
        } else {
            buff.append('\"');
            for (int i = 0, size = value.length(); i < size; i++) {
                char c = value.charAt(i);
                if (c == '\"') {
                    buff.append('\\');
                }
                buff.append(c);
            }
            buff.append('\"');
        }
    }

    /**
     * Append a key-value pair to the string builder. Keys may not contain a
     * colon.
     *
     * @param buff the target buffer
     * @param key the key
     * @param value the value
     */
    public static void appendMap(StringBuilder buff, String key, long value) {
        appendMapKey(buff, key).append(Long.toHexString(value));
    }

    /**
     * Append a key-value pair to the string builder. Keys may not contain a
     * colon.
     *
     * @param buff the target buffer
     * @param key the key
     * @param value the value
     */
    public static void appendMap(StringBuilder buff, String key, int value) {
        appendMapKey(buff, key).append(Integer.toHexString(value));
    }

    /**
     * @param buff output buffer, should be empty
     * @param s parsed string
     * @param i offset to parse from
     * @param size stop offset (exclusive)
     * @return new offset
     */
    private static int parseMapValue(StringBuilder buff, String s, int i, int size) {
        while (i < size) {
            char c = s.charAt(i++);
            if (c == ',') {
                break;
            } else if (c == '\"') {
                while (i < size) {
                    c = s.charAt(i++);
                    if (c == '\\') {
                        if (i == size) {
                            throw newIllegalStateException(ERROR_FILE_CORRUPT, "Not a map: {0}", s);
                        }
                        c = s.charAt(i++);
                    } else if (c == '\"') {
                        break;
                    }
                    buff.append(c);
                }
            } else {
                buff.append(c);
            }
        }
        return i;
    }

    /**
     * Parse a key-value pair list.
     *
     * @param s the list
     * @return the map
     * @throws IllegalStateException if parsing failed
     */
    public static HashMap<String, String> parseMap(String s) {
        HashMap<String, String> map = new HashMap<>();
        StringBuilder buff = new StringBuilder();
        for (int i = 0, size = s.length(); i < size;) {
            int startKey = i;
            i = s.indexOf(':', i);
            if (i < 0) {
                throw newIllegalStateException(ERROR_FILE_CORRUPT, "Not a map: {0}", s);
            }
            String key = s.substring(startKey, i++);
            i = parseMapValue(buff, s, i, size);
            map.put(key, buff.toString());
            buff.setLength(0);
        }
        return map;
    }

    /**
     * Parse a key-value pair list and checks its checksum.
     *
     * @param bytes encoded map
     * @return the map without mapping for {@code "fletcher"}, or {@code null} if checksum is wrong
     * @throws IllegalStateException if parsing failed
     */
    public static HashMap<String, String> parseChecksummedMap(byte[] bytes) {
        int start = 0, end = bytes.length;
        while (start < end && bytes[start] <= ' ') {
            start++;
        }
        while (start < end && bytes[end - 1] <= ' ') {
            end--;
        }
        String s = new String(bytes, start, end - start, StandardCharsets.ISO_8859_1);
        HashMap<String, String> map = new HashMap<>();
        StringBuilder buff = new StringBuilder();
        for (int i = 0, size = s.length(); i < size;) {
            int startKey = i;
            i = s.indexOf(':', i);
            if (i < 0) {
                throw newIllegalStateException(ERROR_FILE_CORRUPT, "Not a map: {0}", s);
            }
            if (i - startKey == 8 && s.regionMatches(startKey, "fletcher", 0, 8)) {
                parseMapValue(buff, s, i + 1, size);
                int check = (int) Long.parseLong(buff.toString(), 16);
                if (check == getFletcher32(bytes, start, startKey - 1)) {
                    return map;
                }
                // Corrupted map
                return null;
            }
            String key = s.substring(startKey, i++);
            i = parseMapValue(buff, s, i, size);
            map.put(key, buff.toString());
            buff.setLength(0);
        }
        // Corrupted map
        return null;
    }

    /**
     * Parse a name from key-value pair list.
     *
     * @param s the list
     * @return value of name item, or {@code null}
     * @throws IllegalStateException if parsing failed
     */
    public static String getMapName(String s) {
        return getFromMap(s, "name");
    }

    /**
     * Parse a specified pair from key-value pair list.
     *
     * @param s the list
     * @param key the name of the key
     * @return value of the specified item, or {@code null}
     * @throws IllegalStateException if parsing failed
     */
    public static String getFromMap(String s, String key) {
        int keyLength = key.length();
        for (int i = 0, size = s.length(); i < size;) {
            int startKey = i;
            i = s.indexOf(':', i);
            if (i < 0) {
                throw newIllegalStateException(ERROR_FILE_CORRUPT, "Not a map: {0}", s);
            }
            if (i++ - startKey == keyLength && s.regionMatches(startKey, key, 0, keyLength)) {
                StringBuilder buff = new StringBuilder();
                parseMapValue(buff, s, i, size);
                return buff.toString();
            } else {
                while (i < size) {
                    char c = s.charAt(i++);
                    if (c == ',') {
                        break;
                    } else if (c == '\"') {
                        while (i < size) {
                            c = s.charAt(i++);
                            if (c == '\\') {
                                if (i++ == size) {
                                    throw newIllegalStateException(ERROR_FILE_CORRUPT, "Not a map: {0}", s);
                                }
                            } else if (c == '\"') {
                                break;
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * Calculate the Fletcher32 checksum.
     *
     * @param bytes the bytes
     * @param offset initial offset
     * @param length the message length (if odd, 0 is appended)
     * @return the checksum
     */
    public static int getFletcher32(byte[] bytes, int offset, int length) {
        int s1 = 0xffff, s2 = 0xffff;
        int i = offset, len = offset + (length & ~1);
        while (i < len) {
            // reduce after 360 words (each word is two bytes)
            for (int end = Math.min(i + 720, len); i < end;) {
                int x = ((bytes[i++] & 0xff) << 8) | (bytes[i++] & 0xff);
                s2 += s1 += x;
            }
            s1 = (s1 & 0xffff) + (s1 >>> 16);
            s2 = (s2 & 0xffff) + (s2 >>> 16);
        }
        if ((length & 1) != 0) {
            // odd length: append 0
            int x = (bytes[i] & 0xff) << 8;
            s2 += s1 += x;
        }
        s1 = (s1 & 0xffff) + (s1 >>> 16);
        s2 = (s2 & 0xffff) + (s2 >>> 16);
        return (s2 << 16) | s1;
    }

    /**
     * Throw an IllegalArgumentException if the argument is invalid.
     *
     * @param test true if the argument is valid
     * @param message the message
     * @param arguments the arguments
     * @throws IllegalArgumentException if the argument is invalid
     */
    public static void checkArgument(boolean test, String message,
            Object... arguments) {
        if (!test) {
            throw newIllegalArgumentException(message, arguments);
        }
    }

    /**
     * Create a new IllegalArgumentException.
     *
     * @param message the message
     * @param arguments the arguments
     * @return the exception
     */
    public static IllegalArgumentException newIllegalArgumentException(
            String message, Object... arguments) {
        return initCause(new IllegalArgumentException(
                formatMessage(0, message, arguments)),
                arguments);
    }

    /**
     * Create a new UnsupportedOperationException.
     *
     * @param message the message
     * @return the exception
     */
    public static UnsupportedOperationException
            newUnsupportedOperationException(String message) {
        return new UnsupportedOperationException(formatMessage(0, message));
    }

    /**
     * Create a new IllegalStateException.
     *
     * @param errorCode the error code
     * @param message the message
     * @param arguments the arguments
     * @return the exception
     */
    public static IllegalStateException newIllegalStateException(
            int errorCode, String message, Object... arguments) {
        return initCause(new IllegalStateException(
                formatMessage(errorCode, message, arguments)),
                arguments);
    }

    private static <T extends Exception> T initCause(T e, Object... arguments) {
        int size = arguments.length;
        if (size > 0) {
            Object o = arguments[size - 1];
            if (o instanceof Throwable) {
                e.initCause((Throwable) o);
            }
        }
        return e;
    }

    /**
     * Format an error message.
     *
     * @param errorCode the error code
     * @param message the message
     * @param arguments the arguments
     * @return the formatted message
     */
    public static String formatMessage(int errorCode, String message,
            Object... arguments) {
        // convert arguments to strings, to avoid locale specific formatting
        arguments = arguments.clone();
        for (int i = 0; i < arguments.length; i++) {
            Object a = arguments[i];
            if (!(a instanceof Exception)) {
                String s = a == null ? "null" : a.toString();
                if (s.length() > 1000) {
                    s = s.substring(0, 1000) + "...";
                }
                arguments[i] = s;
            }
        }
        return MessageFormat.format(message, arguments) +
                " [" + Constants.VERSION_MAJOR + "." +
                Constants.VERSION_MINOR + "." + Constants.BUILD_ID +
                "/" + errorCode + "]";
    }

    /**
     * Get the error code from an exception message.
     *
     * @param m the message
     * @return the error code, or 0 if none
     */
    public static int getErrorCode(String m) {
        if (m != null && m.endsWith("]")) {
            int dash = m.lastIndexOf('/');
            if (dash >= 0) {
                String s = m.substring(dash + 1, m.length() - 1);
                try {
                    return Integer.parseInt(s);
                } catch (NumberFormatException e) {
                    // no error code
                }
            }
        }
        return 0;
    }

    /**
     * Read a hex long value from a map.
     *
     * @param map the map
     * @param key the key
     * @param defaultValue if the value is null
     * @return the parsed value
     * @throws IllegalStateException if parsing fails
     */
    public static long readHexLong(Map<String, ?> map, String key, long defaultValue) {
        Object v = map.get(key);
        if (v == null) {
            return defaultValue;
        } else if (v instanceof Long) {
            return (Long) v;
        }
        try {
            return parseHexLong((String) v);
        } catch (NumberFormatException e) {
            throw newIllegalStateException(ERROR_FILE_CORRUPT,
                    "Error parsing the value {0}", v, e);
        }
    }

    /**
     * Parse an unsigned, hex long.
     *
     * @param x the string
     * @return the parsed value
     * @throws IllegalStateException if parsing fails
     */
    public static long parseHexLong(String x) {
        try {
            if (x.length() == 16) {
                // avoid problems with overflow
                // in Java 8, this special case is not needed
                return (Long.parseLong(x.substring(0, 8), 16) << 32) |
                        Long.parseLong(x.substring(8, 16), 16);
            }
            return Long.parseLong(x, 16);
        } catch (NumberFormatException e) {
            throw newIllegalStateException(ERROR_FILE_CORRUPT,
                    "Error parsing the value {0}", x, e);
        }
    }

    /**
     * Parse an unsigned, hex long.
     *
     * @param x the string
     * @return the parsed value
     * @throws IllegalStateException if parsing fails
     */
    public static int parseHexInt(String x) {
        try {
            // avoid problems with overflow
            // in Java 8, we can use Integer.parseLong(x, 16);
            return (int) Long.parseLong(x, 16);
        } catch (NumberFormatException e) {
            throw newIllegalStateException(ERROR_FILE_CORRUPT,
                    "Error parsing the value {0}", x, e);
        }
    }

    /**
     * Read a hex int value from a map.
     *
     * @param map the map
     * @param key the key
     * @param defaultValue if the value is null
     * @return the parsed value
     * @throws IllegalStateException if parsing fails
     */
    public static int readHexInt(HashMap<String, ?> map, String key, int defaultValue) {
        Object v = map.get(key);
        if (v == null) {
            return defaultValue;
        } else if (v instanceof Integer) {
            return (Integer) v;
        }
        try {
            // support unsigned hex value
            return (int) Long.parseLong((String) v, 16);
        } catch (NumberFormatException e) {
            throw newIllegalStateException(ERROR_FILE_CORRUPT,
                    "Error parsing the value {0}", v, e);
        }
    }

    /**
     * An entry of a map.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    public static final class MapEntry<K, V> implements Map.Entry<K, V> {

        private final K key;
        private final V value;

        public MapEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            throw newUnsupportedOperationException("Updating the value is not supported");
        }

    }

    /**
     * Get the configuration parameter value, or default.
     *
     * @param config the configuration
     * @param key the key
     * @param defaultValue the default
     * @return the configured value or default
     */
    public static int getConfigParam(Map<String, ?> config, String key, int defaultValue) {
        Object o = config.get(key);
        if (o instanceof Number) {
            return ((Number) o).intValue();
        } else if (o != null) {
            try {
                return Integer.decode(o.toString());
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        return defaultValue;
    }

}
