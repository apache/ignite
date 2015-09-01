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

package org.apache.ignite.plugin.extensions.communication;

import org.apache.ignite.plugin.Extension;
import org.jetbrains.annotations.Nullable;

/**
 * Factory for communication messages.
 * <p>
 * A plugin can provide his own message factory as an extension
 * if it uses any custom messages (all message must extend
 * {@link Message} class).
 */
public interface MessageFactory extends Extension {
    /**
     * Creates new message instance of provided type.
     * <p>
     * This method should return {@code null} if provided message type
     * is unknown to this factory.
     *
     * @param type Message type.
     * @return Message instance.
     */
    @Nullable public Message create(byte type);
}