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

package org.apache.ignite.internal.schema.marshaller;

import org.apache.ignite.internal.util.Pair;

/**
 * Key-value objects (de)serializer.
 */
public interface Serializer {
    /**
     * Writes key-value pair to row.
     *
     * @param key Key object.
     * @param val Value object.
     * @return Serialized key-value pair.
     * @throws SerializationException If serialization failed.
     */
    byte[] serialize(Object key, Object val) throws SerializationException;

    /**
     * @param data Key bytes.
     * @param <K> Key object type.
     * @return Key object.
     * @throws SerializationException If deserialization failed.
     */
    <K> K deserializeKey(byte[] data) throws SerializationException;

    /**
     * @param data Value bytes.
     * @param <V> Value object type.
     * @return Value object.
     * @throws SerializationException If deserialization failed.
     */
    <V> V deserializeValue(byte[] data) throws SerializationException;

    /**
     * @param data Row bytes.
     * @param <K> Key object type.
     * @param <V> Value object type.
     * @return Key-value pair.
     * @throws SerializationException If deserialization failed.
     */
    <K, V> Pair<K,V> deserialize(byte[] data) throws SerializationException;
}
