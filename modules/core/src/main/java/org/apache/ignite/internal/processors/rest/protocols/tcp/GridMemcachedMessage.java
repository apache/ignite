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

package org.apache.ignite.internal.processors.rest.protocols.tcp;

import java.util.UUID;
import org.apache.ignite.internal.processors.rest.client.message.GridClientMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Memcached protocol request.
 */
public class GridMemcachedMessage implements GridClientMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Random UUID used for memcached clients authentication. */
    private static final UUID MEMCACHED_ID = UUID.randomUUID();

    /** Header length. */
    public static final int HDR_LEN = 24;

    /** Flags length. */
    public static final byte FLAGS_LENGTH = 4;

    /** Memcache client request flag. */
    public static final byte MEMCACHE_REQ_FLAG = (byte)0x80;

    /** Response flag. */
    public static final byte MEMCACHE_RES_FLAG = (byte)0x81;

    /** Custom client request flag. */
    public static final byte IGNITE_REQ_FLAG = (byte)0x90;

    /** Client handshake flag. */
    public static final byte IGNITE_HANDSHAKE_FLAG = (byte)0x91;

    /** Client handshake flag. */
    public static final byte IGNITE_HANDSHAKE_RES_FLAG = (byte)0x92;

    /** Success status. */
    public static final int SUCCESS = 0x0000;

    /** Key not found status. */
    public static final int KEY_NOT_FOUND = 0x0001;

    /** Failure status. */
    public static final int FAILURE = 0x0004;

    /** Serialized flag. */
    public static final int SERIALIZED_FLAG = 1;

    /** Boolean flag. */
    public static final int BOOLEAN_FLAG = (1 << 8);

    /** Integer flag. */
    public static final int INT_FLAG = (2 << 8);

    /** Long flag. */
    public static final int LONG_FLAG = (3 << 8);

    /** Date flag. */
    public static final int DATE_FLAG = (4 << 8);

    /** Byte flag. */
    public static final int BYTE_FLAG = (5 << 8);

    /** Float flag. */
    public static final int FLOAT_FLAG = (6 << 8);

    /** Double flag. */
    public static final int DOUBLE_FLAG = (7 << 8);

    /** Byte array flag. */
    public static final int BYTE_ARR_FLAG = (8 << 8);

    /** Request flag. */
    private byte reqFlag;

    /** Operation code. */
    private byte opCode;

    /** Key length. */
    private short keyLen;

    /** Extras length. */
    private byte extrasLen;

    /** Status. */
    private int status;

    /** Total body length. */
    private int totalLen;

    /** Opaque. */
    private byte[] opaque;

    /** Extras. */
    private byte[] extras;

    /** Key. */
    private Object key;

    /** Value. */
    private Object val;

    /** Value to add/subtract */
    private Long delta;

    /** Initial value for increment and decrement commands. */
    private Long init;

    /** Expiration time. */
    private Long expiration;

    /** Cache name. */
    private String cacheName;

    /**
     * Creates empty packet which will be filled in parser.
     */
    GridMemcachedMessage() {
    }

    /**
     * Creates copy of request packet for easy response construction.
     *
     * @param req Source request packet.
     */
    GridMemcachedMessage(GridMemcachedMessage req) {
        assert req != null;

        reqFlag = req.reqFlag;
        opCode = req.opCode;

        opaque = new byte[req.opaque.length];
        U.arrayCopy(req.opaque, 0, opaque, 0, req.opaque.length);
    }

    /** {@inheritDoc} */
    @Override public long requestId() {
        return U.bytesToInt(opaque, 0);
    }

    /** {@inheritDoc} */
    @Override public void requestId(long reqId) {
        U.intToBytes((int) reqId, opaque, 0);
    }

    /** {@inheritDoc} */
    @Override public UUID clientId() {
        return MEMCACHED_ID;
    }

    /** {@inheritDoc} */
    @Override public void clientId(UUID id) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public UUID destinationId() {
        return null; // No destination available for memcached packets.
    }

    /** {@inheritDoc} */
    @Override public void destinationId(UUID id) {
        throw new UnsupportedOperationException("destId is not supported by memcached packets.");
    }

    /** {@inheritDoc} */
    @Override public byte[] sessionToken() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void sessionToken(byte[] sesTok) {
        // No-op.
    }

    /**
     * @return Request flag.
     */
    public byte requestFlag() {
        return reqFlag;
    }

    /**
     * @param reqFlag Request flag.
     */
    public void requestFlag(byte reqFlag) {
        this.reqFlag = reqFlag;
    }

    /**
     * @return Operation code.
     */
    public byte operationCode() {
        return opCode;
    }

    /**
     * @param opCode Operation code.
     */
    public void operationCode(byte opCode) {
        assert opCode >= 0;

        this.opCode = opCode;
    }

    /**
     * @return Key length.
     */
    public short keyLength() {
        return keyLen;
    }

    /**
     * @param keyLen Key length.
     */
    public void keyLength(short keyLen) {
        assert keyLen >= 0;

        this.keyLen = keyLen;
    }

    /**
     * @return Extras length.
     */
    public byte extrasLength() {
        return extrasLen;
    }

    /**
     * @param extrasLen Extras length.
     */
    public void extrasLength(byte extrasLen) {
        assert extrasLen >= 0;

        this.extrasLen = extrasLen;
    }

    /**
     * @return Status.
     */
    public int status() {
        return status;
    }

    /**
     * @param status Status.
     */
    public void status(int status) {
        this.status = status;
    }

    /**
     * @return Total length.
     */
    public int totalLength() {
        return totalLen;
    }

    /**
     * @param totalLen Total length.
     */
    public void totalLength(int totalLen) {
        assert totalLen >= 0;

        this.totalLen = totalLen;
    }

    /**
     * @return Opaque.
     */
    public byte[] opaque() {
        return opaque;
    }

    /**
     * @param opaque Opaque.
     */
    public void opaque(byte[] opaque) {
        assert opaque != null;

        this.opaque = opaque;
    }

    /**
     * @return Extras.
     */
    public byte[] extras() {
        return extras;
    }

    /**
     * @param extras Extras.
     */
    public void extras(byte[] extras) {
        assert extras != null;

        this.extras = extras;
    }

    /**
     * @return Key.
     */
    public Object key() {
        return key;
    }

    /**
     * @param key Key.
     */
    public void key(Object key) {
        assert key != null;

        this.key = key;
    }

    /**
     * @return Value.
     */
    public Object value() {
        return val;
    }

    /**
     * @param val Value.
     */
    public void value(Object val) {
        assert val != null;

        this.val = val;
    }

    /**
     * @return Expiration.
     */
    @Nullable public Long expiration() {
        return expiration;
    }

    /**
     * @param expiration Expiration.
     */
    public void expiration(long expiration) {
        this.expiration = expiration;
    }

    /**
     * @return Delta for increment and decrement commands.
     */
    @Nullable public Long delta() {
        return delta;
    }

    /**
     * @param delta Delta for increment and decrement commands.
     */
    public void delta(long delta) {
        this.delta = delta;
    }

    /**
     * @return Initial value for increment and decrement commands.
     */
    @Nullable public Long initial() {
        return init;
    }

    /**
     * @param init Initial value for increment and decrement commands.
     */
    public void initial(long init) {
        this.init = init;
    }

    /**
     * @return Cache name.
     */
    @Nullable public String cacheName() {
        return cacheName;
    }

    /**
     * @param cacheName Cache name.
     */
    public void cacheName(String cacheName) {
        assert cacheName != null;

        this.cacheName = cacheName;
    }

    /**
     * @return Whether request MUST have flags in extras.
     */
    public boolean hasFlags() {
        return opCode == 0x01 ||
            opCode == 0x02 ||
            opCode == 0x03 ||
            opCode == 0x11 ||
            opCode == 0x12 ||
            opCode == 0x13;
    }

    /**
     * @return Whether request has expiration field.
     */
    public boolean hasExpiration() {
        return opCode == 0x01 ||
            opCode == 0x02 ||
            opCode == 0x03 ||
            opCode == 0x11 ||
            opCode == 0x12 ||
            opCode == 0x13;
    }

    /**
     * @return Whether request has delta field.
     */
    public boolean hasDelta() {
        return opCode == 0x05 ||
            opCode == 0x06 ||
            opCode == 0x15 ||
            opCode == 0x16;
    }

    /**
     * @return Whether request has initial field.
     */
    public boolean hasInitial() {
        return opCode == 0x05 ||
            opCode == 0x06 ||
            opCode == 0x15 ||
            opCode == 0x16;
    }

    /**
     * @return Whether to add data to response.
     */
    public boolean addData() {
        return opCode == 0x00 ||
            opCode == 0x05 ||
            opCode == 0x06 ||
            opCode == 0x09 ||
            opCode == 0x0B ||
            opCode == 0x0C ||
            opCode == 0x0D ||
            opCode == 0x20 ||
            opCode == 0x24 ||
            opCode == 0x25 ||
            opCode == 0x26 ||
            opCode == 0x27 ||
            opCode == 0x28 ||
            opCode == 0x29;
    }

    /**
     * @return Whether to add flags to response.
     */
    public boolean addFlags() {
        return opCode == 0x00 ||
            opCode == 0x09 ||
            opCode == 0x0C ||
            opCode == 0x0D;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridMemcachedMessage.class, this);
    }
}