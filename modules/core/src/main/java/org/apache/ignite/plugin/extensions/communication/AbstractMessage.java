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

package org.apache.ignite.plugin.extensions.communication;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.ignite.IgniteException;

import static java.nio.charset.StandardCharsets.UTF_8;

/** Message with generated {@link Message#directType()}. */
public abstract class AbstractMessage implements Message {
    /** Message type. */
    private final short msgType;

    /** Default constructor. */
    protected AbstractMessage() {
        msgType = generateMessageType(getClass());
    }

    /** @param msgTypeStrBasement A string to generate message id with. */
    protected AbstractMessage(String msgTypeStrBasement) {
        msgType = generateMessageType(msgTypeStrBasement);
    }

    /**
     * Generates message id relying on its class.
     *
     * @param msgCls Message class to generate message id with.
     * @return Generated message type.
     */
    public static short generateMessageType(Class<? extends Message> msgCls) {
        return generateMessageType(msgCls.getSimpleName());
    }

    /**
     * Generates message id relying on a string.
     *
     * @param msgTypeStrBasement A string to generate message id with.
     * @return Generated message type.
     */
    public static short generateMessageType(String msgTypeStrBasement) {
        MessageDigest md;

        try {
            md = MessageDigest.getInstance("SHA");
        }
        catch (NoSuchAlgorithmException e) {
            throw new IgniteException("Failed to get digest to generate a message type.", e);
        }

        md.update(msgTypeStrBasement.getBytes(UTF_8));

        byte[] hashBytes = md.digest();

        long msgType = 0;

        for (int i = Math.min(hashBytes.length, 8) - 1; i >= 0; i--)
            msgType = (msgType << 8) | (hashBytes[i] & 0xFF);

        return (short)msgType;
    }

    /** */
    protected short replaceMessageType(short type) {
        return type;
    }

    /** {@inheritDoc} */
    @Override public final short directType() {
        return replaceMessageType(msgType);
    }
}
