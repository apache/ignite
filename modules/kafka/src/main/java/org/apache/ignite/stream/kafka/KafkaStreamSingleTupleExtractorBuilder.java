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

package org.apache.ignite.stream.kafka;

import java.util.Map;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import org.apache.ignite.internal.util.lang.GridMapEntry;
import org.apache.ignite.stream.StreamSingleTupleExtractor;
/**
 * Builds a StreamSingleTupleExtractor that can process a Kafka message with user-injected
 * keyDecoder and messageDecoder instances
 * @param <K> The Type of the Kafka key
 * @param <V> The Type of the Kafka message
 */
public class KafkaStreamSingleTupleExtractorBuilder<K,V>
{
    /**
     * Build the StreamSingleTupleExtractor instance
     * @param keyDecoder The decoder instance for the Kafka key
     * @param messageDecoder The decoder instance for the Kafka message
     * @return The StreamSingleTupleExtractor instance
     */
    public StreamSingleTupleExtractor<MessageAndMetadata<byte[], byte[]>, K, V> build(final Decoder<K> keyDecoder, final Decoder<V> messageDecoder)
    {
        return new StreamSingleTupleExtractor<MessageAndMetadata<byte[], byte[]>, K, V>()
        {
            @Override
            public Map.Entry<K, V> extract(MessageAndMetadata<byte[], byte[]> msg)
            {
                K k = keyDecoder.fromBytes(msg.key());
                V v = messageDecoder.fromBytes(msg.message());
                return new GridMapEntry<>(k, v);
            }
        };
    }
}
