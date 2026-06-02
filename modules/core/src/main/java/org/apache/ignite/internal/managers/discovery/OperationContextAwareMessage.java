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

package org.apache.ignite.internal.managers.discovery;

import org.apache.ignite.internal.OperationContexMessage;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.OperationContextSnapshot;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Wrapping message with effective operation context attributes values.
 *
 * @see OperationContext
 * @see OperationContextSnapshot
 */
public class OperationContextAwareMessage implements Message {
    /** Original wrapped message. */
    @Order(0)
    Message payload;

    /** Effective operation context attributes values. */
    @Order(1)
    OperationContexMessage opCtxMsg;
}
