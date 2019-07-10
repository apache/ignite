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

package org.apache.ignite.plugin.extensions.communication;

import org.apache.ignite.plugin.Extension;

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
    public Message create(short type);
}