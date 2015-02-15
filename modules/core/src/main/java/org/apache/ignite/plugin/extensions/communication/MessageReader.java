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

import java.nio.*;
import java.util.*;

/**
 * Communication message reader.
 * <p>
 * Allows to customize the binary format of communication messages.
 */
public interface MessageReader {
    /**
     * Sets byte buffer to read from.
     *
     * @param buf Byte buffer.
     */
    public void setBuffer(ByteBuffer buf);

    /**
     * Reads field.
     *
     * @param name Field name.
     * @param type Field type.
     * @return Field value.
     */
    public <T> T readField(String name, MessageFieldType type);

    /**
     * Reads array of objects.
     *
     * @param name Field name.
     * @param itemType Array component type.
     * @param itemCls Array component class.
     * @return Array of objects.
     */
    public <T> T[] readArrayField(String name, MessageFieldType itemType, Class<T> itemCls);

    /**
     * Reads collection.
     *
     * @param name Field name.
     * @param itemType Collection item type.
     * @return Collection.
     */
    public <C extends Collection<?>> C readCollectionField(String name, MessageFieldType itemType);

    /**
     * Reads map.
     *
     * @param name Field name.
     * @param keyType Map key type.
     * @param valType Map value type.
     * @param linked Whether {@link LinkedHashMap} should be created.
     * @return Map.
     */
    public <M extends Map<?, ?>> M readMapField(String name, MessageFieldType keyType, MessageFieldType valType,
        boolean linked);

    /**
     * Tells whether last invocation of any of {@code readXXX(...)}
     * methods has fully written the value. {@code False} is returned
     * if there were not enough remaining bytes in byte buffer.
     *
     * @return Whether las value was fully read.
     */
    public boolean isLastRead();
}
