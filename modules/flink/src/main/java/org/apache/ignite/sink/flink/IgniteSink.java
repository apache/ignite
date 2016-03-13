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

package org.apache.ignite.sink.flink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.ignite.*;
import org.apache.ignite.configuration.CollectionConfiguration;

/**
 * Apache Flink Ignite sink implemented as a RichSinkFunction.
 */
public class IgniteSink<IN> extends RichSinkFunction<IN> {

    private static final long serialVersionUID = 1L;

    /** Logger. */
    private IgniteLogger log;

    /**
     * Configuration for Ignite collections.
     */
    private static CollectionConfiguration colCfg;

    private static IgniteQueue queue;
    /**
     * The serialization schema describes how to turn a data object into a different serialized
     * representation.
     *
     */
    private SerializationSchema schema;

    /**
     * Ignite grid configuration file.
     */
    private static String igniteConfigFile;

    /** Queue name. */
    private static String queueName;

    /**
     * Gets the {@link IgniteQueue} queue.
     *
     * @return {@link IgniteQueue} queue.
     */

    public static IgniteQueue getQueue() {
        return SinkContext.getQueue();
    }

    /**
     * Gets the queue name.
     *
     * @return Queue name.
     */
    public String getQueueName() {
        return queueName;
    }

    /**
     * Gets Ignite configuration file.
     *
     * @return Configuration file.
     */
    public String getIgniteConfigFile() {
        return igniteConfigFile;
    }

    public IgniteSink(String queueName,
                      String igniteConfigFile,
                      SerializationSchema schema,
                      CollectionConfiguration colCfg) {
        this.queueName = queueName;
        this.igniteConfigFile = igniteConfigFile;
        this.schema = schema;
        this.colCfg = colCfg;
        this.log = SinkContext.getIgnite().log();
    }

    /**
     * Transfers data into grid. It is called when new data
     * arrives to the sink, and forwards it to {@link IgniteQueue}
     *
     * @param in IN.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void invoke(IN in) {

        try {
            Object object = schema.serialize(in);
            queue = SinkContext.getQueue();
            queue.put(object);
        } catch (Exception e) {
            log.error("Error while processing IN of " + queueName, e);
        }
    }

    public static class SinkContext {

        /** Constructor.*/
        private SinkContext() {
        }

        /** Instance holder.*/
        private static class Holder {
            private static final Ignite IGNITE = Ignition.start(igniteConfigFile);
            private static final IgniteQueue QUEUE = IGNITE.queue(queueName, 0, colCfg);
        }

        /**
         * Obtains grid instance.
         *
         * @return Grid instance.
         */
        public static Ignite getIgnite() {
            return Holder.IGNITE;
        }

        /**
         * Obtains data streamer instance.
         *
         * @return Data streamer instance.
         */
        public static IgniteQueue getQueue() {
            return Holder.QUEUE;
        }
    }
}
