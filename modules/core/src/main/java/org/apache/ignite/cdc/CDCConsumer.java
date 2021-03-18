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

package org.apache.ignite.cdc;

import java.util.Iterator;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * Consumer of data change events.
 */
@IgniteExperimental
public interface CDCConsumer<K, V> {
    /**
     * @return Consumer ID.
     */
    String id();

    /**
     * Starts the consumer.
     *
     * @param configuration Ignite configuration.
     */
    void start(IgniteConfiguration configuration, IgniteLogger log);

    /**
     * @return {@code True} if entry key and value should be keeped in binary format.
     */
    boolean keepBinary();

    /**
     * Handles entry changes events.
     * If this method return {@code true} then current offset will be stored and ongoing notifications after CDC application fail/restart
     * will be started from it.
     *
     * @param events Entry change events.
     * @return {@code True} if current offset should be committed.
     */
    boolean onChange(Iterator<EntryEvent<K, V>> events);

    /**
     * Stops the consumer.
     * This methods can be invoked only after {@link #start(IgniteConfiguration, IgniteLogger)}.
     */
    void stop();
}
