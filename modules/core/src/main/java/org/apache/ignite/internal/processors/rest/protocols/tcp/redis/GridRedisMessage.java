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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.rest.client.message.GridClientMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Message to communicate with Redis client. Contains command, its attributes and response.
 */
public class GridRedisMessage implements GridClientMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Random UUID used for REDIS clients authentication. */
    private static final UUID RESP_ID = UUID.randomUUID();

    /** Request byte. */
    public static final byte RESP_REQ_FLAG = GridRedisProtocolParser.ARRAY;

    /** Command index. */
    private static final int CMD_POS = 0;

    /** Key index. */
    private static final int KEY_POS = 1;

    /** Auxiliary parameters offset. */
    private static final int AUX_OFFSET = 2;

    /** Request message parts. */
    private final transient List<String> msgParts;

    /** Response. */
    private ByteBuffer response;

    /** Cache name. */
    private String cacheName;

    /** Cache name prefix. */
    public static final String CACHE_NAME_PREFIX = "redis-ignite-internal-cache";

    /** Default cache name. */
    public static final String DFLT_CACHE_NAME = CACHE_NAME_PREFIX + "-0";

    /**
     * Constructor.
     *
     * @param msgLen Length of the Redis message (command with parameters).
     */
    public GridRedisMessage(int msgLen) {
        msgParts = new ArrayList<>(msgLen);

        cacheName = DFLT_CACHE_NAME;
    }

    /**
     * Appends the specified part to the message.
     *
     * @param part Part to append.
     */
    public void append(String part) {
        msgParts.add(part);
    }

    /**
     * Sets the response.
     *
     * @param response Response.
     */
    public void setResponse(ByteBuffer response) {
        this.response = response;
    }

    /**
     * Gets the response.
     *
     * @return Response.
     */
    public ByteBuffer getResponse() {
        return response;
    }

    /**
     * Gets all message parts.
     *
     * @return Message elements.
     */
    private List<String> getMsgParts() {
        return msgParts;
    }

    /**
     * @return Number of elements in the message.
     */
    public int messageSize() {
        return msgParts.size();
    }

    /**
     * @return {@link GridRedisCommand}.
     */
    public GridRedisCommand command() {
        return GridRedisCommand.valueOf(msgParts.get(CMD_POS).toUpperCase());
    }

    /**
     * @return Key for the command.
     */
    public String key() {
        if (msgParts.size() <= KEY_POS)
            return null;

        return msgParts.get(KEY_POS);
    }

    /**
     * @return Parameters by index if available.
     */
    public String aux(int idx) {
        if (msgParts.size() <= idx)
            return null;

        return msgParts.get(idx);
    }

    /**
     * @return All parameters if available.
     */
    public List<String> aux() {
        if (msgParts.size() <= AUX_OFFSET)
            return null;

        return msgParts.subList(AUX_OFFSET, msgParts.size());
    }

    /**
     * @return All parameters for multi-key commands if available.
     */
    public List<String> auxMKeys() {
        if (msgParts.size() <= KEY_POS)
            return null;

        return msgParts.subList(KEY_POS, msgParts.size());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRedisMessage.class, this);
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

    /** {@inheritDoc} */
    @Override public long requestId() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public void requestId(long reqId) {

    }

    /** {@inheritDoc} */
    @Override public UUID clientId() {
        return RESP_ID;
    }

    /** {@inheritDoc} */
    @Override public void clientId(UUID id) {
        throw new IgniteException("Setting client id is not expected!");
    }

    /** {@inheritDoc} */
    @Override public UUID destinationId() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void destinationId(UUID id) {

    }

    /** {@inheritDoc} */
    @Override public byte[] sessionToken() {
        return new byte[0];
    }

    /** {@inheritDoc} */
    @Override public void sessionToken(byte[] sesTok) {

    }
}
