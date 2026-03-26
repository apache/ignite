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

package org.apache.ignite.spi.communication;

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Test message class.
 */
public class TestMessage2 extends GridCacheMessage {
    /** */
    public static final short DIRECT_TYPE = 201;

    /** Node id. */
    @Order(0)
    UUID nodeId;

    /** Integer field. */
    @Order(1)
    int id;

    /** Body. */
    @Order(2)
    String body;

    /** */
    @Order(3)
    Message msg;

    /** @param mes Message. */
    public void init(Message mes, UUID nodeId, int id, String body) {
        this.nodeId = nodeId;
        this.id = id;
        this.msg = mes;
        this.body = body;
    }

    /** {@inheritDoc} */
    @Override public boolean addDeploymentInfo() {
        return false;
    }

    /** @return Body.*/
    public String body() {
        return body;
    }

    /** @return Message. */
    public Message message() {
        return msg;
    }

    /** @return Node id. */
    public UUID nodeId() {
        return nodeId;
    }

    /** @return Id. */
    public int id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return DIRECT_TYPE;
    }
}
