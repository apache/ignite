package org.apache.ignite.stream.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.stream.StreamAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * Server that subscribes to topic messages from Kafka broker and streams its to key-value pairs into
 * {@link IgniteDataStreamer} instance.
 * <p>
 * Uses Kafka's High Level Consumer API to read messages from Kafka. 
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/Consumer+Group+Example">Consumer Consumer Group
 * Example</a>
 */

public class KafkaDataStreamer<K, V> extends StreamAdapter<MessageAndMetadata<byte[], byte[]>, K, V> {

	private static Logger log = LoggerFactory.getLogger(KafkaDataStreamer.class);
	/** Retry timeout. */
    private static final long DFLT_RETRY_TIMEOUT = 10000;

    /** Executor used to submit kafka streams. */
    private ExecutorService executor;

    /** Topic. */
    private String topic;

    /** Number of threads to process kafka streams. */
    private int threads;

    /** Kafka consumer config. */
    private ConsumerConfig consumerCfg;
    
    /** Kafka consumer connector. */
    private ConsumerConnector consumer;
   
    /** Retry timeout. */
    private long retryTimeout = DFLT_RETRY_TIMEOUT;

    /** Stopped. */
    private volatile boolean stopped;
    
    /**
     * Sets the topic name.
     *
     * @param topic Topic name.
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * Sets the threads.
     *
     * @param threads Number of threads.
     */
    public void setThreads(int threads) {
        this.threads = threads;
    }

    /**
     * Sets the consumer config.
     *
     * @param consumerCfg Consumer configuration.
     */
    public void setConsumerConfig(ConsumerConfig consumerCfg) {
        this.consumerCfg = consumerCfg;
    }

    /**
     * Sets the retry timeout.
     *
     * @param retryTimeout Retry timeout.
     */
    public void setRetryTimeout(long retryTimeout) {
       A.ensure(retryTimeout > 0, "retryTimeout > 0");
    	if (retryTimeout <= 0){
    		throw new IllegalArgumentException("retryTimeout must be greater than 0");
    	}
        this.retryTimeout = retryTimeout;
    }

    /**
     * Starts streamer.
     *
     * @throws IgniteException If failed.
     */
    public void start() {
    	 A.notNull(getStreamer(), "streamer");
         A.notNull(getIgnite(), "ignite");
         A.notNull(topic, "topic");
         A.notNull(consumerCfg, "kafka consumer config");
         A.ensure(threads > 0, "threads > 0");
         A.ensure(null != getSingleTupleExtractor() || null != getMultipleTupleExtractor() , "Extractor must be configured");

        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerCfg);
//        kafka.javaapi.consumer.ConsumerConnector createJavaConsumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(consumerCfg);

        Map<String, Integer> topicCntMap = new HashMap<>();

        topicCntMap.put(topic, threads);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCntMap);

        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        
        log.info("Kafka is connected successfully");        
        // Now launch all the consumer threads.
        executor = Executors.newFixedThreadPool(threads);

        stopped = false;

        // Now create an object to consume the messages.
        for (final KafkaStream<byte[], byte[]> stream : streams) {
            executor.submit(new Runnable() {
                @Override public void run() {
                    while (!stopped) {
                        try {
                        	MessageAndMetadata<byte[], byte[]> msg = null;
                            for (ConsumerIterator<byte[], byte[]> it = stream.iterator(); it.hasNext() && !stopped; ) {
                                msg = it.next();
                                try {
                                	addMessage(msg);
                                }
                                catch (Exception e) {
                                    log.error("Message is ignored due to an error [msg=" + msg + ']', e);
                                }
                            }
                        }
                        catch (Exception e) {
                            log.error("Message can't be consumed from stream. Retry after " + retryTimeout + " ms.", e);

                            try {
                                Thread.sleep(retryTimeout);
                            }
                            catch (InterruptedException ie) {
                                // No-op.
                            }
                        }
                    }
                }
            });
        }
    }

    /**
     * Stops streamer.
     */
    public void stop() {
        stopped = true;

        if (consumer != null)
            consumer.shutdown();

        if (executor != null) {
            executor.shutdown();

            try {
                if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS))
                    if (log.isDebugEnabled())
                        log.debug("Timed out waiting for consumer threads to shut down, exiting uncleanly.");
            }
            catch (InterruptedException e) {
                if (log.isDebugEnabled())
                    log.debug("Interrupted during shutdown, exiting uncleanly.");
            }
        }
    }
    
}
