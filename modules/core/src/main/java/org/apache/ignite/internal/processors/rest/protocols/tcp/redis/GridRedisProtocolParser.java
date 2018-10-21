/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.rest.protocols.tcp.redis;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.rest.protocols.tcp.GridTcpRestParser;

/**
 * Parser to decode/encode Redis protocol (RESP) requests.
 */
public class GridRedisProtocolParser {
    /** + prefix. */
    private static final byte SIMPLE_STRING = 43;

    /** $ */
    private static final byte BULK_STRING = 36;

    /** : */
    private static final byte INTEGER = 58;

    /** * */
    static final byte ARRAY = 42;

    /** - */
    private static final byte ERROR = 45;

    /** Carriage return code. */
    private static final byte CR = 13;

    /** Line feed code. */
    private static final byte LF = 10;

    /** CRLF. */
    private static final byte[] CRLF = new byte[] {13, 10};

    /** Generic error prefix. */
    private static final byte[] ERR_GENERIC = "ERR ".getBytes();

    /** Prefix for errors on operations with the wrong type. */
    private static final byte[] ERR_TYPE = "WRONGTYPE ".getBytes();

    /** Prefix for errors on authentication. */
    private static final byte[] ERR_AUTH = "NOAUTH ".getBytes();

    /** Null bulk string for nil response. */
    private static final byte[] NIL = "$-1\r\n".getBytes();

    /** OK response. */
    private static final byte[] OK = "OK".getBytes();

    /**
     * Reads an array into {@link GridRedisMessage}.
     *
     * @param buf Buffer.
     * @return {@link GridRedisMessage}.
     * @throws IgniteCheckedException
     */
    public static GridRedisMessage readArray(ByteBuffer buf) throws IgniteCheckedException {
        byte b = buf.get();

        if (b != ARRAY)
            throw new IgniteCheckedException("Invalid request byte! " + b);

        int arrLen = elCnt(buf);

        GridRedisMessage msg = new GridRedisMessage(arrLen);

        for (int i = 0; i < arrLen; i++)
            msg.append(readBulkStr(buf));

        return msg;
    }

    /**
     * Reads a bulk string.
     *
     * @param buf Buffer.
     * @return Bulk string.
     * @throws IgniteCheckedException
     */
    public static String readBulkStr(ByteBuffer buf) throws IgniteCheckedException {
        byte b = buf.get();

        if (b != BULK_STRING)
            throw new IgniteCheckedException("Invalid bulk string prefix! " + b);

        int len = elCnt(buf);
        byte[] bulkStr = new byte[len];

        buf.get(bulkStr, 0, len);

        if (buf.get() != CR || buf.get() != LF)
            throw new IgniteCheckedException("Invalid request syntax!");

        return new String(bulkStr);
    }

    /*
     * A validation method to check packet completeness.
     * return true if and only if
     * 1. First byte is ARRAY (43)
     * 2. Last two bytes are CR(13) LF(10)
     *
     * Otherwise, return false representing this is an incomplete packet with three possible scenarios:
     * 1. A beginning packet with leading ARRAY byte
     * 2. A continual packet with ending CRLF bytes.
     * 3. A continual packet with neither conditions above.
     */
    public static boolean validatePacket(ByteBuffer buf) {
        return validatePacketHeader(buf) && validatePacketFooter(buf);
    }

    public static boolean validatePacketHeader(ByteBuffer buf) {
        boolean result = true;

        //mark at initial position
        buf.mark();

        if(buf.get() != ARRAY) {
            result = false;
        }

        //reset to initial position
        buf.reset();

        return result;
    }

    public static boolean validatePacketFooter(ByteBuffer buf) {
        boolean result = true;

        //mark at initial position
        buf.mark();


        int limit = buf.limit();

        assert limit > 2;

        //check the final CR(last -2 ) and LF(last -1) byte
        if (buf.get(limit - 2) != CR || buf.get(limit - 1) != LF) {
            result = false;
        }


        //reset to initial position
        buf.reset();

        return result;
    }

    /**
     * Counts elements in buffer.
     *
     * @param buf Buffer.
     * @return Count of elements.
     */
    private static int elCnt(ByteBuffer buf) throws IgniteCheckedException {
        byte[] arrLen = new byte[9];

        int idx = 0;
        byte b = buf.get();
        while (b != CR) {
            arrLen[idx++] = b;
            b = buf.get();
        }

        if (buf.get() != LF)
            throw new IgniteCheckedException("Invalid request syntax!");

        return Integer.parseInt(new String(arrLen, 0, idx));
    }

    /**
     * Converts a simple string data to a {@link ByteBuffer}.
     *
     * @param val String to be converted to a simple string.
     * @return Redis simple string.
     */
    public static ByteBuffer toSimpleString(String val) {
        byte[] b = val.getBytes();

        return toSimpleString(b);
    }

    /**
     * Creates a simple string data as a {@link ByteBuffer}.
     *
     * @param b Bytes for a simple string.
     * @return Redis simple string.
     */
    public static ByteBuffer toSimpleString(byte[] b) {
        ByteBuffer buf = ByteBuffer.allocate(b.length + 3);
        buf.put(SIMPLE_STRING);
        buf.put(b);
        buf.put(CRLF);

        buf.flip();

        return buf;
    }

    /**
     * @return Standard OK string.
     */
    public static ByteBuffer oKString() {
        return toSimpleString(OK);
    }

    /**
     * Creates a generic error response.
     *
     * @param errMsg Error message.
     * @return Error response.
     */
    public static ByteBuffer toGenericError(String errMsg) {
        return toError(errMsg, ERR_GENERIC);
    }

    /**
     * Creates an error response on operation against the wrong data type.
     *
     * @param errMsg Error message.
     * @return Error response.
     */
    public static ByteBuffer toTypeError(String errMsg) {
        return toError(errMsg, ERR_TYPE);
    }

    /**
     * Creates an error response.
     *
     * @param errMsg Error message.
     * @param errPrefix Error prefix.
     * @return Error response.
     */
    private static ByteBuffer toError(String errMsg, byte[] errPrefix) {
        byte[] b = errMsg.getBytes();

        ByteBuffer buf = ByteBuffer.allocate(b.length + errPrefix.length + 3);
        buf.put(ERROR);
        buf.put(errPrefix);
        buf.put(b);
        buf.put(CRLF);

        buf.flip();

        return buf;
    }

    /**
     * Converts an integer result to a RESP integer.
     *
     * @param integer Integer result.
     * @return REDIS integer.
     */
    public static ByteBuffer toInteger(String integer) {
        byte[] b = integer.getBytes();

        ByteBuffer buf = ByteBuffer.allocate(b.length + 3);
        buf.put(INTEGER);
        buf.put(b);
        buf.put(CRLF);

        buf.flip();

        return buf;
    }

    /**
     * Converts an integer result to a RESP integer.
     *
     * @param integer Integer result.
     * @return REDIS integer.
     */
    public static ByteBuffer toInteger(int integer) {
        return toInteger(String.valueOf(integer));
    }

    /**
     * Creates Nil response.
     *
     * @return Nil response.
     */
    public static ByteBuffer nil() {
        ByteBuffer buf = ByteBuffer.allocate(NIL.length);
        buf.put(NIL);

        buf.flip();

        return buf;
    }

    /**
     * Converts a resultant object to a bulk string.
     *
     * @param val Object.
     * @return Bulk string.
     */
    public static ByteBuffer toBulkString(Object val) {
        assert val != null;

        byte[] b = String.valueOf(val).getBytes();
        byte[] l = String.valueOf(b.length).getBytes();

        ByteBuffer buf = ByteBuffer.allocate(b.length + l.length + 5);
        buf.put(BULK_STRING);
        buf.put(l);
        buf.put(CRLF);
        buf.put(b);
        buf.put(CRLF);

        buf.flip();

        return buf;
    }

    /**
     * Converts a resultant map response to an array.
     *
     * @param vals Map.
     * @return Array response.
     */
    public static ByteBuffer toArray(Map<Object, Object> vals) {
        return toArray(vals.values());
    }

    /**
     * Converts a resultant collection response to an array.
     *
     * @param vals Array elements.
     * @return Array response.
     */
    public static ByteBuffer toArray(Collection<Object> vals) {
        assert vals != null;

        byte[] arrSize = String.valueOf(vals.size()).getBytes();

        ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024);
        buf.put(ARRAY);
        buf.put(arrSize);
        buf.put(CRLF);

        for (Object val : vals)
            buf.put(toBulkString(val));

        buf.flip();

        return buf;
    }
}
