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
    public StreamSingleTupleExtractor<MessageAndMetadata<byte[], byte[]>, K, V> build(Decoder<K> keyDecoder, Decoder<V> messageDecoder)
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
