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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Serializer based on {@link JdkMarshaller}.
 */
public class CacheEventSerializer implements Serializer<CacheEvent> {
    /** Marshaller. */
    private static final Marshaller marsh = new JdkMarshaller();

    /** {@inheritDoc} */
    @Override public void configure(Map<String, ?> map, boolean b) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public byte[] serialize(String topic, CacheEvent event) {
        try {
            return U.marshal(marsh, event);
        }
        catch (IgniteCheckedException e) {
            throw new SerializationException("Failed to serialize cache event!", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // No-op.
    }
}
