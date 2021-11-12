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

import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.util.Pair;

/**
 * Key-value objects (de)serializer.
 *
 * @deprecated see {@link org.apache.ignite.internal.schema.marshaller.reflection.Marshaller}
 */
@Deprecated(forRemoval = true)
public interface Serializer {
    /**
     * Writes key-value pair to row.
     *
     * @param key Key object.
     * @param val Value object.
     * @return Binary row.
     * @throws MarshallerException If serialization failed.
     */
    BinaryRow serialize(Object key, Object val) throws MarshallerException;

    /**
     * DeserializeKey.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @param row Row.
     * @param <K> Key object type.
     * @return Key object.
     * @throws MarshallerException If deserialization failed.
     */
    <K> K deserializeKey(Row row) throws MarshallerException;

    /**
     * DeserializeValue.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @param row Row.
     * @param <V> Value object type.
     * @return Value object.
     * @throws MarshallerException If deserialization failed.
     */
    <V> V deserializeValue(Row row) throws MarshallerException;

    /**
     * Deserialize.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @param row Row.
     * @param <K> Key object type.
     * @param <V> Value object type.
     * @return Key-value pair.
     * @throws MarshallerException If deserialization failed.
     */
    <K, V> Pair<K, V> deserialize(Row row) throws MarshallerException;

    /**
     * Get schema descriptor.
     */
    SchemaDescriptor schema();
}
