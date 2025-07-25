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
package org.apache.ignite.internal.processors.marshaller;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Client node receives discovery messages in asynchronous mode
 * so it is possible that all server nodes already accepted new mapping but clients are unaware about it.
 *
 * In this case it is possible for client node to receive a request to perform some operation on such class
 * client doesn't know about its mapping.
 * Upon receiving such request client sends an explicit {@link MissingMappingRequestMessage} mapping request
 * to one of server nodes using CommunicationSPI and waits for {@link MissingMappingResponseMessage} response.
 *
 * If server node where mapping request was sent to leaves the cluster for some reason
 * mapping request gets automatically resent to the next alive server node in topology.
 */
public class MissingMappingRequestMessage implements Message {
    /** */
    @Order(0)
    private byte platformId;

    /** */
    @Order(1)
    private int typeId;

    /**
     * Default constructor.
     */
    public MissingMappingRequestMessage() {
        //No-op.
    }

    /**
     * @param platformId Platform id.
     * @param typeId Type id.
     */
    MissingMappingRequestMessage(byte platformId, int typeId) {
        this.platformId = platformId;
        this.typeId = typeId;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 78;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /** */
    public byte platformId() {
        return platformId;
    }

    /** */
    public void platformId(byte platformId) {
        this.platformId = platformId;
    }

    /** */
    public int typeId() {
        return typeId;
    }

    /** */
    public void typeId(int typeId) {
        this.typeId = typeId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MissingMappingRequestMessage.class, this);
    }
}
