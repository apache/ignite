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

package org.apache.ignite.stream.jms11;

import java.util.Map;
import javax.jms.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Implement this interface to transform from a {@link Message} to a set of cache entries in the form of a {@link Map}.
 *
 * @param <T> The type of JMS Message.
 * @param <K> The type of the cache key.
 * @param <V> The type of the cache value.
 * @author Raul Kripalani
 */
public interface MessageTransformer<T extends Message, K, V> {

    /**
     * Transformation function.
     *
     * @param message The message received from the JMS broker.
     * @return Set of cache entries to add to the cache. It could be empty or null if the message should be skipped.
     */
    @Nullable Map<K, V> apply(T message);

}