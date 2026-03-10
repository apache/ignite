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

/** */
public class MessageArrayType implements MessageType {
    /** */
    private final MessageType valType;

    /** */
    private final Class<?> clazz;

    /**
     * @param valueType Value type.
     * @param clazz Class.
     */
    public MessageArrayType(MessageType valType, Class<?> clazz) {
        this.valType = valType;
        this.clazz = clazz;
    }

    /** @return Value type. */
    public MessageType valueType() {
        return valType;
    }

    /** {@inheritDoc} */
    @Override public MessageCollectionItemType type() {
        return MessageCollectionItemType.ARRAY;
    }

    /** @return Class. */
    public Class clazz() {
        return clazz;
    }
}
