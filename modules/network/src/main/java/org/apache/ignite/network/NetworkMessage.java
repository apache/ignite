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
package org.apache.ignite.network;

/**
 * Message for exchange information in cluster.
 */
public class NetworkMessage {
    /** Custom data. */
    private final Object data;

    /** Network member who sent this message. */
    private final NetworkMember senderMember;

    /**
     * @param data Custom data.
     * @param senderMember Network member who sent this message.
     */
    public NetworkMessage(Object data, NetworkMember senderMember) {
        this.data = data;
        this.senderMember = senderMember;
    }

    /**
     * @param <T> Type of message.
     * @return Custom data.
     */
    public <T> T data() {
        return (T)data;
    }

    /**
     * @return Network member who sent this message.
     */
    public NetworkMember sender() {
        return senderMember;
    }

    @Override public String toString() {
        return "NetworkMessage{" +
            "data=" + data +
            ", senderMember=" + senderMember +
            '}';
    }
}
