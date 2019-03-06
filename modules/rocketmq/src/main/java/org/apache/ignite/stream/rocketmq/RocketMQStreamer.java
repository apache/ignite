/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.stream.rocketmq;

import java.util.List;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.stream.StreamAdapter;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * Streamer that subscribes to a RocketMQ topic amd feeds messages into {@link IgniteDataStreamer} instance.
 */
public class RocketMQStreamer<K, V> extends StreamAdapter<List<MessageExt>, K, V> implements MessageListenerConcurrently {
    /** Logger. */
    private IgniteLogger log;

    /** RocketMQ consumer. */
    private DefaultMQPushConsumer consumer;

    /** State. */
    private volatile boolean stopped = true;

    /** Topic to subscribe to. */
    private String topic;

    /** Consumer group. */
    private String consumerGrp;

    /** Name server address. */
    private String nameSrvAddr;

    /**
     * Starts streamer.
     *
     * @throws IgniteException If failed.
     */
    public void start() {
        if (!stopped)
            throw new IgniteException("Attempted to start an already started RocketMQ streamer");

        // validate parameters.
        A.notNull(getStreamer(), "streamer");
        A.notNull(getIgnite(), "ignite");
        A.notNull(topic, "topic");
        A.notNull(consumerGrp, "consumer group");
        A.notNullOrEmpty(nameSrvAddr, "nameserver address");
        A.ensure(null != getMultipleTupleExtractor(), "Multiple tuple extractor must be configured");

        log = getIgnite().log();

        consumer = new DefaultMQPushConsumer(consumerGrp);

        consumer.setNamesrvAddr(nameSrvAddr);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        try {
            consumer.subscribe(topic, "*");
        }
        catch (MQClientException e) {
            throw new IgniteException("Failed to subscribe to " + topic, e);
        }

        consumer.registerMessageListener(this);

        try {
            consumer.start();
        }
        catch (MQClientException e) {
            throw new IgniteException("Failed to start the streamer", e);
        }

        stopped = false;
    }

    /**
     * Stops streamer.
     */
    public void stop() {
        if (consumer != null)
            consumer.shutdown();

        stopped = true;
    }

    /**
     * Implements {@link MessageListenerConcurrently#consumeMessage(List, ConsumeConcurrentlyContext)} to receive
     * messages.
     *
     * {@inheritDoc}
     */
    @Override public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
        ConsumeConcurrentlyContext context) {
        if (log.isDebugEnabled())
            log.debug("Received " + msgs.size() + " messages");

        addMessage(msgs);

        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    /**
     * Sets the topic to subscribe to.
     *
     * @param topic The topic to subscribe to.
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * Sets the name of the consumer group.
     *
     * @param consumerGrp Consumer group name.
     */
    public void setConsumerGrp(String consumerGrp) {
        this.consumerGrp = consumerGrp;
    }

    /**
     * Sets the name server address.
     *
     * @param nameSrvAddr Name server address
     */
    public void setNameSrvAddr(String nameSrvAddr) {
        this.nameSrvAddr = nameSrvAddr;
    }
}
