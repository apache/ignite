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
 * On receiving a {@link MissingMappingRequestMessage} mapping request server node looks up class name
 * for requested platformId and typeId in its local marshaller cache and sends back
 * a {@link MissingMappingResponseMessage} mapping response with resolved class name.
 */
public class MissingMappingResponseMessage implements Message {
    /** */
    @Order(0)
    private byte platformId;

    /** */
    @Order(1)
    private int typeId;

    /** */
    @Order(value = 2, method = "className")
    private String clsName;

    /**
     * Default constructor.
     */
    public MissingMappingResponseMessage() {
    }

    /**
     * @param platformId Platform id.
     * @param typeId Type id.
     * @param clsName Class name.
     */
    MissingMappingResponseMessage(byte platformId, int typeId, String clsName) {
        this.platformId = platformId;
        this.typeId = typeId;
        this.clsName = clsName;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 79;
    }

    /**
     *
     */
    public byte platformId() {
        return platformId;
    }

    /** */
    public void platformId(byte platformId) {
        this.platformId = platformId;
    }

    /**
     *
     */
    public int typeId() {
        return typeId;
    }

    /** */
    public void typeId(int typeId) {
        this.typeId = typeId;
    }

    /**
     *
     */
    public String className() {
        return clsName;
    }

    /** */
    public void className(String clsName) {
        this.clsName = clsName;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MissingMappingResponseMessage.class, this);
    }
}
