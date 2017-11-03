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

package org.apache.ignite.cache.store.cassandra.serializer;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Interface which should be implemented by all serializers responsible
 * for writing/loading data to/from Cassandra in binary (BLOB) format.
 */
public interface Serializer extends Serializable {
    /**
     * Serializes object into byte buffer.
     *
     * @param obj Object to serialize.
     * @return Byte buffer with binary data.
     */
    public ByteBuffer serialize(Object obj);

    /**
     * Deserializes object from byte buffer.
     *
     * @param buf Byte buffer.
     * @return Deserialized object.
     */
    public Object deserialize(ByteBuffer buf);
}
