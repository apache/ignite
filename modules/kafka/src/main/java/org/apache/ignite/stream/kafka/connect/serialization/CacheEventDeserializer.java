/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Deserializer based on {@link JdkMarshaller}.
 */
public class CacheEventDeserializer implements Deserializer<CacheEvent> {
    /** Marshaller. */
    private static final Marshaller marsh = new JdkMarshaller();

    /** {@inheritDoc} */
    @Override public void configure(Map<String, ?> map, boolean b) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public CacheEvent deserialize(String topic, byte[] bytes) {
        try {
            return U.unmarshal(marsh, bytes, getClass().getClassLoader());
        }
        catch (IgniteCheckedException e) {
            throw new SerializationException("Failed to deserialize cache event!", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // No-op.
    }
}
