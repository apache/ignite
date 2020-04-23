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

package org.apache.ignite.stream.kafka.connect.serialization;

import java.util.Map;
import org.apache.ignite.events.CacheEvent;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

/**
 * {@link CacheEvent} converter for Connect API.
 */
public class CacheEventConverter implements Converter {
    private final CacheEventDeserializer deserializer = new CacheEventDeserializer();

    private final CacheEventSerializer serializer = new CacheEventSerializer();

    /** {@inheritDoc} */
    @Override public void configure(Map<String, ?> map, boolean b) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public byte[] fromConnectData(String topic, Schema schema, Object o) {
        try {
            return serializer.serialize(topic, (CacheEvent)o);
        }
        catch (SerializationException e) {
            throw new DataException("Failed to convert to byte[] due to a serialization error", e);
        }
    }

    /** {@inheritDoc} */
    @Override public SchemaAndValue toConnectData(String topic, byte[] bytes) {
        CacheEvent evt;

        try {
            evt = deserializer.deserialize(topic, bytes);
        }
        catch (SerializationException e) {
            throw new DataException("Failed to convert to Kafka Connect data due to a serialization error", e);
        }

        if (evt == null) {
            return SchemaAndValue.NULL;
        }
        return new SchemaAndValue(null, evt);
    }
}
