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
public class MessageMapType implements MessageType {
    /** */
    private final MessageType keyType;

    /** */
    private final MessageType valueType;

    /** */
    private final boolean linked;

    /**
     * @param keyType Key type.
     * @param valueType Value type.
     * @param linked Is linked hash map.
     */
    public MessageMapType(MessageType keyType, MessageType valType, boolean linked) {
        this.keyType = keyType;
        this.valType = valType;
        this.linked = linked;
    }

    /** @return Key type. */
    public MessageType keyType() {
        return keyType;
    }

    /** @return Value type. */
    public MessageType valueType() {
        return valueType;
    }

    /** {@inheritDoc} */
    @Override public MessageCollectionItemType type() {
        return MessageCollectionItemType.MAP;
    }

    /** @return Is linked hash map. */
    public boolean linked() {
        return linked;
    }
}
