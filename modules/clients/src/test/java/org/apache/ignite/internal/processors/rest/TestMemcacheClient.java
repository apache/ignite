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

package org.apache.ignite.internal.processors.rest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.java.JavaLogger;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Test client.
 */
final class TestMemcacheClient {
    /** Header length. */
    private static final short HDR_LEN = 24;

    /** Serialized flag. */
    private static final short SERIALIZED_FLAG = 1;

    /** Boolean flag. */
    private static final short BOOLEAN_FLAG = (1 << 8);

    /** Integer flag. */
    private static final short INT_FLAG = (2 << 8);

    /** Long flag. */
    private static final short LONG_FLAG = (3 << 8);

    /** Date flag. */
    private static final short DATE_FLAG = (4 << 8);

    /** Byte flag. */
    private static final short BYTE_FLAG = (5 << 8);

    /** Float flag. */
    private static final short FLOAT_FLAG = (6 << 8);

    /** Double flag. */
    private static final short DOUBLE_FLAG = (7 << 8);

    /** Byte array flag. */
    private static final short BYTE_ARR_FLAG = (8 << 8);

    /** Logger. */
    private final IgniteLogger log = new JavaLogger();

    /** JDK marshaller. */
    private final Marshaller jdkMarshaller = new JdkMarshaller();

    /** Socket. */
    private final Socket sock;

    /** Opaque counter. */
    private final AtomicInteger opaqueCntr = new AtomicInteger(0);

    /** Response queue. */
    private final BlockingQueue<Response> queue =
        new LinkedBlockingQueue<>();

    /** Socket reader. */
    private final Thread rdr;

    /** Quit response. */
    private static final Response QUIT_RESP = new Response(0, false, null, null);

    /**
     * Creates client.
     *
     * @param host Hostname.
     * @param port Port number.
     * @throws IgniteCheckedException In case of error.
     */
    TestMemcacheClient(String host, int port) throws IgniteCheckedException {
        assert host != null;
        assert port > 0;

        try {
            sock = new Socket(host, port);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to establish connection.", e);
        }

        // Start socket reader thread.
        rdr = new Thread(new Runnable() {
            @SuppressWarnings("InfiniteLoopStatement")
            @Override public void run() {
                try {
                    InputStream in = sock.getInputStream();

                    ByteArrayOutputStream buf = new ByteArrayOutputStream();

                    boolean running = true;

                    while (running) {
                        byte opCode = 0;
                        byte extrasLength = 0;
                        int keyLength = 0;
                        boolean success = false;
                        int totalLength = 0;
                        int opaque = 0;
                        short keyFlags = 0;
                        short valFlags = 0;
                        Object obj = null;
                        Object key = null;

                        int i = 0;

                        while (true) {
                            int symbol = in.read();

                            if (symbol == -1) {
                                running = false;

                                break;
                            }

                            byte b = (byte)symbol;

                            if (i == 1)
                                opCode = b;
                            if (i == 2 || i == 3) {
                                buf.write(b);

                                if (i == 3) {
                                    keyLength = U.bytesToShort(buf.toByteArray(), 0);

                                    buf.reset();
                                }
                            }
                            else if (i == 4)
                                extrasLength = b;
                            else if (i == 6 || i == 7) {
                                buf.write(b);

                                if (i == 7) {
                                    success = U.bytesToShort(buf.toByteArray(), 0) == 0;

                                    buf.reset();
                                }
                            }
                            else if (i >= 8 && i <= 11) {
                                buf.write(b);

                                if (i == 11) {
                                    totalLength = U.bytesToInt(buf.toByteArray(), 0);

                                    buf.reset();
                                }
                            }
                            else if (i >= 12 && i <= 15) {
                                buf.write(b);

                                if (i == 15) {
                                    opaque = U.bytesToInt(buf.toByteArray(), 0);

                                    buf.reset();
                                }
                            }
                            else if (i >= HDR_LEN && i < HDR_LEN + extrasLength) {
                                buf.write(b);

                                if (i == HDR_LEN + extrasLength - 1) {
                                    byte[] rawFlags = buf.toByteArray();

                                    keyFlags = U.bytesToShort(rawFlags, 0);
                                    valFlags = U.bytesToShort(rawFlags, 2);

                                    buf.reset();
                                }
                            }
                            else if (i >= HDR_LEN + extrasLength && i < HDR_LEN + extrasLength + keyLength) {
                                buf.write(b);

                                if (i == HDR_LEN + extrasLength + keyLength - 1) {
                                    key = decode(buf.toByteArray(), keyFlags);

                                    buf.reset();
                                }
                            }
                            else if (i >= HDR_LEN + extrasLength + keyLength && i < HDR_LEN + totalLength) {
                                buf.write(b);

                                if (opCode == 0x05 || opCode == 0x06)
                                    valFlags = LONG_FLAG;

                                if (i == HDR_LEN + totalLength - 1) {
                                    obj = decode(buf.toByteArray(), valFlags);

                                    buf.reset();
                                }
                            }

                            if (i == HDR_LEN + totalLength - 1) {
                                queue.add(new Response(opaque, success, key, obj));

                                break;
                            }

                            i++;
                        }
                    }
                }
                catch (IOException e) {
                    if (!Thread.currentThread().isInterrupted())
                        U.error(log, e);
                }
                catch (Exception e) {
                    U.error(log, e);
                }
                finally {
                    U.closeQuiet(sock);

                    queue.add(QUIT_RESP);
                }
            }
        });

        rdr.start();
    }

    /** {@inheritDoc} */
    public void shutdown() throws IgniteCheckedException {
        try {
            if (rdr != null) {
                rdr.interrupt();

                U.closeQuiet(sock);

                rdr.join();
            }
        }
        catch (InterruptedException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * Makes request to server and waits for response.
     *
     * @param cmd Command.
     * @param cacheName Cache name.
     * @param key Key.
     * @param val Value.
     * @param extras Extras.
     * @return Response.
     * @throws IgniteCheckedException In case of error.
     */
    private Response makeRequest(
        Command cmd,
        @Nullable String cacheName,
        @Nullable Object key,
        @Nullable Object val,
        @Nullable Long... extras
    ) throws IgniteCheckedException {
        assert cmd != null;

        int opaque = opaqueCntr.getAndIncrement();

        // Send request.
        try {
            sock.getOutputStream().write(createPacket(cmd, cacheName, key, val, opaque, extras));
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to send packet.", e);
        }

        // Wait for response.
        while (true) {
            try {
                // Take response from queue.
                Response res = queue.take();

                if (res == QUIT_RESP)
                    return res;

                // Check opaque value.
                if (res.getOpaque() == opaque) {
                    if (!res.isSuccess() && res.getObject() != null)
                        throw new IgniteCheckedException((String)res.getObject());
                    else
                        return res;
                }
                else
                    // Return response to queue if opaque is incorrect.
                    queue.add(res);
            }
            catch (InterruptedException e) {
                throw new IgniteCheckedException("Interrupted while waiting for response.", e);
            }
        }
    }

    /**
     * Makes request to server and waits for response.
     *
     * @param cmd Command.
     * @param cacheName Cache name.
     * @param key Key.
     * @param val Value.
     * @param extras Extras.
     * @return Response.
     * @throws IgniteCheckedException In case of error.
     */
    private List<Response> makeMultiRequest(
        Command cmd,
        @Nullable String cacheName,
        @Nullable Object key,
        @Nullable Object val,
        @Nullable Long... extras
    ) throws IgniteCheckedException {
        assert cmd != null;

        int opaque = opaqueCntr.getAndIncrement();

        List<Response> resList = new LinkedList<>();

        // Send request.
        try {
            sock.getOutputStream().write(createPacket(cmd, cacheName, key, val, opaque, extras));
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to send packet.", e);
        }

        // Wait for response.
        while (true) {
            try {
                // Take response from queue.
                Response res = queue.take();

                if (res == QUIT_RESP)
                    return resList;

                // Check opaque value.
                if (res.getOpaque() == opaque) {
                    if (!res.isSuccess() && res.getObject() != null)
                        throw new IgniteCheckedException((String)res.getObject());
                    else {
                        if (res.getObject() == null)
                            return resList;

                        resList.add(res);
                    }
                }
                else
                    // Return response to queue if opaque is incorrect.
                    queue.add(res);
            }
            catch (InterruptedException e) {
                throw new IgniteCheckedException("Interrupted while waiting for response.", e);
            }
        }
    }

    /**
     * Creates packet.
     *
     * @param cmd Command.
     * @param cacheName Cache name.
     * @param key Key.
     * @param val Value.
     * @param opaque Opaque.
     * @param extras Extras.
     * @throws IgniteCheckedException In case of error.
     * @return Packet.
     */
    private byte[] createPacket(
        Command cmd,
        @Nullable String cacheName,
        @Nullable Object key,
        @Nullable Object val,
        int opaque,
        @Nullable Long[] extras
    ) throws IgniteCheckedException {
        assert cmd != null;
        assert opaque >= 0;

        byte[] cacheNameBytes = cacheName != null ? cacheName.getBytes() : null;

        Data keyData = encode(key);

        Data valData = encode(val);

        int cacheNameLength = cacheNameBytes != null ? cacheNameBytes.length : 0;
        int extrasLength = cmd.extrasLength() + cacheNameLength;

        byte[] packet = new byte[HDR_LEN + extrasLength + keyData.length() + valData.length()];

        packet[0] = (byte)0x80;
        packet[1] = cmd.operationCode();

        U.shortToBytes((short) keyData.length(), packet, 2);

        packet[4] = (byte)(extrasLength);

        U.intToBytes(extrasLength + keyData.length() + valData.length(), packet, 8);
        U.intToBytes(opaque, packet, 12);

        if (extrasLength > 0) {
            if (extras != null) {
                int offset = HDR_LEN;

                for (Long extra : extras) {
                    if (extra != null)
                        U.longToBytes(extra, packet, offset);

                    offset += 8;
                }
            }
            else {
                U.shortToBytes(keyData.getFlags(), packet, HDR_LEN);
                U.shortToBytes(valData.getFlags(), packet, HDR_LEN + 2);
            }
        }

        if (cacheNameBytes != null)
            U.arrayCopy(cacheNameBytes, 0, packet, HDR_LEN + cmd.extrasLength(), cacheNameLength);

        if (keyData.getBytes() != null)
            U.arrayCopy(keyData.getBytes(), 0, packet, HDR_LEN + extrasLength, keyData.length());

        if (valData.getBytes() != null)
            U.arrayCopy(valData.getBytes(), 0, packet, HDR_LEN + extrasLength + keyData.length(), valData.length());

        return packet;
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @param val Value.
     * @return If value was actually put.
     * @throws IgniteCheckedException In case of error.
     */
    public <K, V> boolean cachePut(@Nullable String cacheName, K key, V val)
        throws IgniteCheckedException {
        assert key != null;
        assert val != null;

        return makeRequest(Command.PUT, cacheName, key, val).isSuccess();
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @return Value.
     * @throws IgniteCheckedException In case of error.
     */
    public <K, V> V cacheGet(@Nullable String cacheName, K key)
        throws IgniteCheckedException {
        assert key != null;

        return makeRequest(Command.GET, cacheName, key, null).getObject();
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @return Whether entry was actually removed.
     * @throws IgniteCheckedException In case of error.
     */
    public <K> boolean cacheRemove(@Nullable String cacheName, K key) throws IgniteCheckedException {
        assert key != null;

        return makeRequest(Command.REMOVE, cacheName, key, null).isSuccess();
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @param val Value.
     * @return Whether entry was added.
     * @throws IgniteCheckedException In case of error.
     */
    public <K, V> boolean cacheAdd(@Nullable String cacheName, K key, V val)
        throws IgniteCheckedException {
        assert key != null;
        assert val != null;

        return makeRequest(Command.ADD, cacheName, key, val).isSuccess();
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @param val Value.
     * @return Whether value was actually replaced.
     * @throws IgniteCheckedException In case of error.
     */
    public <K, V> boolean cacheReplace(@Nullable String cacheName, K key, V val)
        throws IgniteCheckedException {
        assert key != null;
        assert val != null;

        return makeRequest(Command.REPLACE, cacheName, key, val).isSuccess();
    }

    /**
     * @param cacheName Cache name.
     * @throws IgniteCheckedException In case of error.
     * @return Metrics map.
     */
    public <K> Map<String, Long> cacheMetrics(@Nullable String cacheName) throws IgniteCheckedException {
        List<Response> raw = makeMultiRequest(Command.CACHE_METRICS, cacheName, null, null);

        Map<String, Long> res = new HashMap<>(raw.size());

        for (Response resp : raw)
            res.put((String)resp.key(), Long.parseLong(String.valueOf(resp.getObject())));

        return res;
    }

    /**
     * @param key Key.
     * @param init Initial value (optional).
     * @param incr Amount to add.
     * @return New value.
     * @throws IgniteCheckedException In case of error.
     */
    public <K> long increment(K key, @Nullable Long init, long incr)
        throws IgniteCheckedException {
        assert key != null;

        return makeRequest(Command.INCREMENT, null, key, null, incr, init).<Long>getObject();
    }

    /**
     * @param key Key.
     * @param init Initial value (optional).
     * @param decr Amount to subtract.
     * @return New value.
     * @throws IgniteCheckedException In case of error.
     */
    public <K> long decrement(K key, @Nullable Long init, long decr)
        throws IgniteCheckedException {
        assert key != null;

        return makeRequest(Command.DECREMENT, null, key, null, decr, init).<Long>getObject();
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @param val Value to append.
     * @return Whether operation succeeded.
     * @throws IgniteCheckedException In case of error.
     */
    public <K> boolean cacheAppend(@Nullable String cacheName, K key, String val)
        throws IgniteCheckedException {
        assert key != null;
        assert val != null;

        return makeRequest(Command.APPEND, cacheName, key, val).isSuccess();
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @param val Value to prepend.
     * @return Whether operation succeeded.
     * @throws IgniteCheckedException In case of error.
     */
    public <K> boolean cachePrepend(@Nullable String cacheName, K key, String val)
        throws IgniteCheckedException {
        assert key != null;
        assert val != null;

        return makeRequest(Command.PREPEND, cacheName, key, val).isSuccess();
    }

    /**
     * @return Version.
     * @throws IgniteCheckedException In case of error.
     */
    public String version() throws IgniteCheckedException {
        return makeRequest(Command.VERSION, null, null, null).getObject();
    }

    /**
     * @throws IgniteCheckedException In case of error.
     */
    public void noop() throws IgniteCheckedException {
        Response res = makeRequest(Command.NOOP, null, null, null);

        assert res != null;
        assert res.isSuccess();
        assert res.getObject() == null;
    }

    /**
     * @throws IgniteCheckedException In case of error.
     */
    public void quit() throws IgniteCheckedException {
        makeRequest(Command.QUIT, null, null, null);

        assert sock.isClosed();
    }

    /**
     * Encodes object.
     *
     * @param obj Object.
     * @return Encoded data.
     * @throws IgniteCheckedException In case of error.
     */
    public Data encode(@Nullable Object obj) throws IgniteCheckedException {
        if (obj == null)
            return new Data(null, (short)0);

        byte[] bytes;
        short flags = 0;

        if (obj instanceof String)
            bytes = ((String)obj).getBytes();
        else if (obj instanceof Boolean) {
            bytes = new byte[] {(byte)((Boolean)obj ? '1' : '0')};

            flags |= BOOLEAN_FLAG;
        }
        else if (obj instanceof Integer) {
            bytes = U.intToBytes((Integer) obj);

            flags |= INT_FLAG;
        }
        else if (obj instanceof Long) {
            bytes = U.longToBytes((Long) obj);

            flags |= LONG_FLAG;
        }
        else if (obj instanceof Date) {
            bytes = U.longToBytes(((Date) obj).getTime());

            flags |= DATE_FLAG;
        }
        else if (obj instanceof Byte) {
            bytes = new byte[] {(Byte)obj};

            flags |= BYTE_FLAG;
        }
        else if (obj instanceof Float) {
            bytes = U.intToBytes(Float.floatToIntBits((Float) obj));

            flags |= FLOAT_FLAG;
        }
        else if (obj instanceof Double) {
            bytes = U.longToBytes(Double.doubleToLongBits((Double) obj));

            flags |= DOUBLE_FLAG;
        }
        else if (obj instanceof byte[]) {
            bytes = (byte[])obj;

            flags |= BYTE_ARR_FLAG;
        }
        else {
            bytes = jdkMarshaller.marshal(obj);

            flags |= SERIALIZED_FLAG;
        }

        return new Data(bytes, flags);
    }

    /**
     * @param bytes Byte array to decode.
     * @param flags Flags.
     * @return Decoded value.
     * @throws IgniteCheckedException In case of error.
     */
    public Object decode(byte[] bytes, short flags) throws IgniteCheckedException {
        assert bytes != null;
        assert flags >= 0;

        if ((flags & SERIALIZED_FLAG) != 0)
            return jdkMarshaller.unmarshal(bytes, getClass().getClassLoader());

        int masked = flags & 0xff00;

        switch (masked) {
            case BOOLEAN_FLAG:
                return bytes[0] == '1';
            case INT_FLAG:
                return U.bytesToInt(bytes, 0);
            case LONG_FLAG:
                return U.bytesToLong(bytes, 0);
            case DATE_FLAG:
                return new Date(U.bytesToLong(bytes, 0));
            case BYTE_FLAG:
                return bytes[0];
            case FLOAT_FLAG:
                return Float.intBitsToFloat(U.bytesToInt(bytes, 0));
            case DOUBLE_FLAG:
                return Double.longBitsToDouble(U.bytesToLong(bytes, 0));
            case BYTE_ARR_FLAG:
                return bytes;
            default:
                return new String(bytes);
        }
    }

    /**
     * Response data.
     */
    private static class Response {
        /** Opaque. */
        private final int opaque;

        /** Success flag. */
        private final boolean success;

        /** Key. */
        private final Object key;

        /** Response object. */
        private final Object obj;

        /**
         * @param opaque Opaque.
         * @param success Success flag.
         * @param key Key object.
         * @param obj Response object.
         */
        Response(int opaque, boolean success, @Nullable Object key, @Nullable Object obj) {
            assert opaque >= 0;

            this.opaque = opaque;
            this.success = success;
            this.key = key;
            this.obj = obj;
        }

        /**
         * @return Opaque.
         */
        int getOpaque() {
            return opaque;
        }

        /**
         * @return Success flag.
         */
        boolean isSuccess() {
            return success;
        }

        Object key() {
            return key;
        }

        /**
         * @return Response object.
         */
        @SuppressWarnings("unchecked")
        <T> T getObject() {
            return (T)obj;
        }
    }


    private static class Data {
        /** Bytes. */
        private final byte[] bytes;

        /** Flags. */
        private final short flags;

        /**
         * @param bytes Bytes.
         * @param flags Flags.
         */
        Data(@Nullable byte[] bytes, short flags) {
            assert flags >= 0;

            this.bytes = bytes;
            this.flags = flags;
        }

        /**
         * @return Bytes.
         */
        @Nullable public byte[] getBytes() {
            return bytes;
        }

        /**
         * @return Flags.
         */
        public short getFlags() {
            return flags;
        }

        /**
         * @return Length.
         */
        public int length() {
            return bytes != null ? bytes.length : 0;
        }
    }

    /**
     * Command.
     */
    private enum Command {
        /** Get. */
        GET((byte)0x00, 4),

        /** Put. */
        PUT((byte)0x01, 8),

        /** Add. */
        ADD((byte)0x02, 8),

        /** Replace. */
        REPLACE((byte)0x03, 8),

        /** Remove. */
        REMOVE((byte)0x04, 4),

        /** Increment. */
        INCREMENT((byte)0x05, 20),

        /** Decrement. */
        DECREMENT((byte)0x06, 20),

        /** Quit. */
        QUIT((byte)0x07, 0),

        /** Cache metrics. */
        CACHE_METRICS((byte)0x10, 4),

        /** No-op. */
        NOOP((byte)0x0A, 0),

        /** Version. */
        VERSION((byte)0x0B, 0),

        /** Append. */
        APPEND((byte)0x0E, 4),

        /** Append. */
        PREPEND((byte)0x0F, 4);

        /** Operation code. */
        private final byte opCode;

        /** Extras length. */
        private final int extrasLength;

        /**
         * @param opCode Operation code.
         * @param extrasLength Extras length.
         */
        Command(byte opCode, int extrasLength) {
            this.opCode = opCode;
            this.extrasLength = extrasLength;
        }

        /**
         * @return Operation code.
         */
        public byte operationCode() {
            return opCode;
        }

        /**
         * @return Extras length.
         */
        public int extrasLength() {
            return extrasLength;
        }
    }
}