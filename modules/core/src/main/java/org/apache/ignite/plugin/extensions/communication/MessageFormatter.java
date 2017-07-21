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

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.plugin.Extension;

/**
 * Provides a custom format for communication messages.
 * <p>
 * A plugin can provide his own message factory as an extension
 * to replace default format of communication messages.
 * <p>
 * Note that only one custom formatter is allowed. If two
 * plugins provide different formatters, exception will
 * be thrown on node startup.
 */
public interface MessageFormatter extends Extension {
    /**
     * Creates new message writer instance.
     *
     * @param rmtNodeId Remote node ID.
     * @return Message writer.
     * @throws IgniteCheckedException In case of error.
     */
    public MessageWriter writer(UUID rmtNodeId) throws IgniteCheckedException;

    /**
     * Creates new message reader instance.
     *
     * @param rmtNodeId Remote node ID.
     * @param msgFactory Message factory.
     * @return Message reader.
     * @throws IgniteCheckedException In case of error.
     */
    public MessageReader reader(UUID rmtNodeId, MessageFactory msgFactory) throws IgniteCheckedException;
}
